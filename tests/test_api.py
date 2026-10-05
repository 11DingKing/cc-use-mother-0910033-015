"""HTTP 接口测试：批准端点幂等、租户隔离、管理端点与端到端投递。"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from notification_outbox.api import create_server
from notification_outbox.clock import ManualClock
from notification_outbox.db import Database
from notification_outbox.sender import RecordingSender
from notification_outbox.worker import OutboxWorker

TOKENS = {"tok-school-1": "school-1", "tok-school-2": "school-2"}
ADMIN = "ops-token"


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = str(Path(self.tmp.name) / "outbox.db")
        self.clock = ManualClock()
        self.server = create_server(
            self.db_path,
            tenant_tokens=TOKENS,
            admin_tokens={ADMIN},
            clock=self.clock,
        )
        self.port = self.server.server_address[1]
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(thread.join, 2)

    def call(self, method: str, path: str, body: dict | None = None, token: str | None = None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            method=method,
            data=json.dumps(body).encode("utf-8") if body is not None else None,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def approve_body(self, school_id: str = "school-1") -> dict:
        return {
            "school_id": school_id,
            "docent_id": "docent-7",
            "venue_id": "venue-3",
            "activity_date": "2026-10-12 上午",
            "approved_by": "活动统筹员-01",
        }

    def test_approve_and_replay(self) -> None:
        status, body = self.call("POST", "/api/changes/CHG-1/approve", self.approve_body(), "tok-school-1")
        self.assertEqual(status, 201)
        self.assertFalse(body["replayed"])
        # 超时重试同一批准请求：幂等返回原事件，不产生重复通知
        status, replay = self.call("POST", "/api/changes/CHG-1/approve", self.approve_body(), "tok-school-1")
        self.assertEqual(status, 200)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["event_id"], body["event_id"])
        # 同一变更号但内容不一致：冲突
        changed = self.approve_body()
        changed["venue_id"] = "venue-9"
        status, _ = self.call("POST", "/api/changes/CHG-1/approve", changed, "tok-school-1")
        self.assertEqual(status, 409)

    def test_approve_requires_auth_and_own_tenant(self) -> None:
        status, _ = self.call("POST", "/api/changes/CHG-2/approve", self.approve_body())
        self.assertEqual(status, 401)
        status, _ = self.call(
            "POST", "/api/changes/CHG-2/approve", self.approve_body("school-2"), "tok-school-1"
        )
        self.assertEqual(status, 403)

    def test_notifications_are_tenant_isolated(self) -> None:
        _, body1 = self.call("POST", "/api/changes/CHG-1/approve", self.approve_body("school-1"), "tok-school-1")
        _, body2 = self.call("POST", "/api/changes/CHG-2/approve", self.approve_body("school-2"), "tok-school-2")

        # 学校令牌访问其他学校的通知：拒绝，且不返回任何内容
        status, denied = self.call("GET", "/api/tenants/school-2/notifications", token="tok-school-1")
        self.assertEqual(status, 403)
        self.assertNotIn("notifications", denied)
        status, _ = self.call("GET", "/api/tenants/school-1/notifications")
        self.assertEqual(status, 401)

        # 各自只能看到自己的通知内容
        status, mine = self.call("GET", "/api/tenants/school-1/notifications", token="tok-school-1")
        self.assertEqual(status, 200)
        self.assertEqual([n["event_id"] for n in mine["notifications"]], [body1["event_id"]])
        self.assertNotIn(body2["event_id"], json.dumps(mine, ensure_ascii=False))
        status, other = self.call("GET", "/api/tenants/school-2/notifications", token="tok-school-2")
        self.assertEqual([n["event_id"] for n in other["notifications"]], [body2["event_id"]])

    def test_admin_endpoints_require_admin_token(self) -> None:
        self.call("POST", "/api/changes/CHG-1/approve", self.approve_body(), "tok-school-1")
        status, _ = self.call("GET", "/api/admin/backlog", token="tok-school-1")
        self.assertEqual(status, 403)
        status, backlog = self.call("GET", "/api/admin/backlog", token=ADMIN)
        self.assertEqual(status, 200)
        self.assertEqual(backlog["events"]["pending"], 1)
        # 积压视图只含计数，不含任何通知内容
        self.assertNotIn("content", json.dumps(backlog))

    def test_redrive_endpoint(self) -> None:
        _, body = self.call("POST", "/api/changes/CHG-1/approve", self.approve_body(), "tok-school-1")
        event_id = body["event_id"]
        # 学校令牌不能重投
        status, _ = self.call(
            "POST", f"/api/admin/events/{event_id}/redrive", {"reason": "x"}, "tok-school-1"
        )
        self.assertEqual(status, 403)
        # 未终止失败的事件不能重投
        status, _ = self.call(
            "POST", f"/api/admin/events/{event_id}/redrive", {"reason": "x"}, ADMIN
        )
        self.assertEqual(status, 409)
        # 置为终止失败后可重投
        db = Database(self.db_path)
        with db.transaction() as conn:
            conn.execute("UPDATE outbox_event SET status = 'dead' WHERE event_id = ?", (event_id,))
            conn.execute("UPDATE delivery SET status = 'dead' WHERE event_id = ?", (event_id,))
        status, result = self.call(
            "POST", f"/api/admin/events/{event_id}/redrive",
            {"actor": "ops", "reason": "通道恢复"}, ADMIN,
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "pending")
        status, _ = self.call(
            "POST", "/api/admin/events/evt_missing/redrive", {"reason": "x"}, ADMIN
        )
        self.assertEqual(status, 404)

    def test_end_to_end_approve_worker_delivers_school_reads(self) -> None:
        _, body = self.call("POST", "/api/changes/CHG-1/approve", self.approve_body(), "tok-school-1")
        sender = RecordingSender()
        worker = OutboxWorker(
            Database(self.db_path), sender, clock=self.clock, worker_id="w1",
            lease_seconds=30, base_backoff_seconds=10,
        )
        self.assertEqual(worker.run_until_idle()["done"], 1)
        self.assertEqual(len(sender.messages), 3)

        status, mine = self.call("GET", "/api/tenants/school-1/notifications", token="tok-school-1")
        self.assertEqual(status, 200)
        note = mine["notifications"][0]
        self.assertEqual(note["event_id"], body["event_id"])
        self.assertEqual(note["status"], "done")
        self.assertTrue(all(d["status"] == "delivered" for d in note["deliveries"]))
        titles = {d["recipient_role"]: d["content"]["title"] for d in note["deliveries"]}
        self.assertEqual(
            titles,
            {"docent": "新讲解任务", "school_contact": "参观活动已排定", "venue_admin": "场馆排期确认"},
        )


if __name__ == "__main__":
    unittest.main()
