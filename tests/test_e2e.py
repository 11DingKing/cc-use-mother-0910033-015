"""端到端测试：CLI 子进程与真实 HTTP 套接字。"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

import support  # noqa: F401  （导入即把 src 加入 sys.path）

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
ENV = {**os.environ, "PYTHONPATH": str(SRC)}


def run_cli(*args: str, db: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "notification_outbox", "backlog", "--db", str(db)],
        capture_output=True, text=True, env=ENV, check=False,
    )


def cli(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "notification_outbox", *args],
        capture_output=True, text=True, env=ENV, check=False,
    )


SEED_SCRIPT = """
import sys
from notification_outbox.database import connect, initialize, transaction
from notification_outbox.service import initialize_business, create_change, approve_change

db = sys.argv[1]
conn = connect(db)
data = {
    "school_id": 1, "venue_id": 10, "docent_id": 100,
    "school_contact": "school@a.edu", "docent_contact": "docent@guide.cn",
    "venue_contact": "venue@museum.cn",
    "school_name": "第一中学", "venue_name": "城市博物馆", "docent_name": "李讲解",
    "visit_date": "2026-10-20", "start_time": "09:30",
}
cid = create_change(conn, data)
with transaction(conn):
    approve_change(conn, cid, school_id=1)
print(cid)
conn.close()
"""


class CliEndToEndTest(unittest.TestCase):
    def test_init_backlog_worker_reinject(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "app.sqlite3"
            spool = Path(tmp) / "sent.log"

            r = cli(["init-db", "--db", str(db)])
            self.assertEqual(r.returncode, 0, r.stderr)

            seed = subprocess.run(
                [sys.executable, "-c", SEED_SCRIPT, str(db)],
                capture_output=True, text=True, env=ENV, check=False,
            )
            self.assertEqual(seed.returncode, 0, seed.stderr)

            r = cli(["backlog", "--db", str(db)])
            self.assertEqual(r.returncode, 0, r.stderr)
            summary = json.loads(r.stdout)
            self.assertEqual(summary["pending"], 3)
            self.assertEqual(summary["due_now"], 3)

            worker = subprocess.Popen(
                [sys.executable, "-m", "notification_outbox", "worker",
                 "--db", str(db), "--spool", str(spool),
                 "--poll-interval", "0.2"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=ENV,
            )
            try:
                # 等待三条消息全部进入投递日志。
                for _ in range(100):
                    if spool.exists() and len(spool.read_text().splitlines()) >= 3:
                        break
                    import time
                    time.sleep(0.1)
                lines = spool.read_text().splitlines()
                self.assertEqual(len(lines), 3)
                messages = [json.loads(x) for x in lines]
                self.assertEqual(
                    {m["channel"] for m in messages},
                    {"school", "docent", "venue"},
                )
                # 不同接收方内容确实不同。
                subjects = {m["subject"] for m in messages}
                self.assertEqual(len(subjects), 3)
            finally:
                worker.terminate()
                worker.wait(timeout=10)
                if worker.stdout is not None:
                    worker.stdout.close()

            r = cli(["backlog", "--db", str(db)])
            summary = json.loads(r.stdout)
            self.assertEqual(summary["succeeded"], 3)

            # 制造一条 dead，验证 list / reinject 管理命令。
            mark = subprocess.run(
                [sys.executable, "-c",
                 "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]);"
                 "c.execute(\"UPDATE outbox_recipient SET status='dead' "
                 "WHERE channel='venue'\"); c.commit()",
                 str(db)],
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(mark.returncode, 0, mark.stderr)

            r = cli(["list", "--db", str(db), "--status", "dead"])
            self.assertEqual(r.returncode, 0, r.stderr)
            dead_items = json.loads(r.stdout)
            self.assertEqual(len(dead_items), 1)
            rid = dead_items[0]["id"]

            r = cli(["reinject", "--db", str(db), "--actor", "ops-wang",
                     "--note", "人工补发", str(rid)])
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(json.loads(r.stdout)["reinjected"], [rid])

            # 重投非 dead 项应返回非零。
            other = cli(["list", "--db", str(db), "--status", "succeeded"])
            sid = json.loads(other.stdout)[0]["id"]
            r = cli(["reinject", "--db", str(db), "--actor", "ops-wang", str(sid)])
            self.assertEqual(r.returncode, 1)


class HttpEndToEndTest(unittest.TestCase):
    def test_http_server_auth_and_isolation(self) -> None:
        import threading
        from http.client import HTTPConnection

        from notification_outbox.api import (
    OutboxApi, Principal, connection_scope, serve,
)
        from notification_outbox.database import (
            connect, initialize, transaction,
        )
        from notification_outbox.service import (
            approve_change, create_change, initialize_business,
        )

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "http.sqlite3"
            setup = connect(db)
            initialize(setup)
            initialize_business(setup)
            data = {
                "school_id": 1, "venue_id": 10, "docent_id": 100,
                "school_contact": "school@a.edu", "docent_contact": "docent@guide.cn",
                "venue_contact": "venue@museum.cn",
                "school_name": "第一中学", "venue_name": "城市博物馆",
                "docent_name": "李讲解",
                "visit_date": "2026-10-20", "start_time": "09:30",
            }
            cid = create_change(setup, data)
            with transaction(setup):
                approve_change(setup, cid, school_id=1)
            setup.close()

            tokens = {
                "s1": Principal("s1", 1, False),
                "s2": Principal("s2", 2, False),
                "admin": Principal("admin", None, True),
            }
            api = OutboxApi(lambda: connection_scope(db), tokens)
            server = serve(api, host="127.0.0.1", port=0)
            port = server.server_address[1]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                # 无令牌 401。
                conn = HTTPConnection("127.0.0.1", port, timeout=5)
                conn.request("GET", "/api/recipients")
                self.assertEqual(conn.getresponse().status, 401)
                conn.close()

                # 学校令牌只能看到本校 3 条。
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/api/recipients",
                    headers={"Authorization": "Bearer s1"},
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    body = json.loads(resp.read())
                    self.assertEqual(resp.status, 200)
                    self.assertEqual(len(body["items"]), 3)

                # 管理端可看全量积压。
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/admin/backlog",
                    headers={"Authorization": "Bearer admin"},
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    self.assertEqual(json.loads(resp.read())["pending"], 3)

                # 学校令牌访问管理端 403。
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/admin/backlog",
                    headers={"Authorization": "Bearer s1"},
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(req, timeout=5)
                self.assertEqual(ctx.exception.code, 403)
            finally:
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    unittest.main()
