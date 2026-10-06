"""命令行入口：初始化库、查看积压、人工重投、运行工作进程与 HTTP 服务。

用法示例::

    python -m notification_outbox init-db --db /var/lib/outbox/app.sqlite3
    python -m notification_outbox backlog --db ...
    python -m notification_outbox list-dead --db ...
    python -m notification_outbox reinject --db ... --actor zhang 12 15
    python -m notification_outbox worker --db ... --poll-interval 2
    python -m notification_outbox serve --db ... --port 8080
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from .api import OutboxApi, Principal, connection_scope, serve
from .database import connect, initialize
from .models import RecipientStatus
from .rendering import TemplateRenderer
from .repositories import backlog_summary, list_recipients
from .senders import OutboundMessage
from .worker import DeliveryWorker, reinject_dead


class LoggingSender:
    """默认发送器：把已渲染消息以 JSON Lines 追加到投递日志。

    生产环境替换为真实短信/邮件/Webhook 适配器；替换时保持
    成功/失败的异常约定（见 senders.Sender）。
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, message: OutboundMessage) -> None:
        record = {
            "ts": time.time(),
            "recipient_id": message.recipient_id,
            "channel": message.channel,
            "address": message.address,
            "subject": message.subject,
            "body": message.body,
            "dedupe_key": message.dedupe_key,
        }
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())


def _default_spool(db_path: str) -> str:
    return str(Path(db_path).with_suffix(".sent.log"))


def cmd_init_db(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    try:
        initialize(conn)
        from .service import initialize_business

        initialize_business(conn)
    finally:
        conn.close()
    print(f"已初始化：{args.db}")
    return 0


def cmd_backlog(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    try:
        summary = backlog_summary(conn)
    finally:
        conn.close()
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


def cmd_list_dead(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    try:
        rows = list_recipients(
            conn, status=RecipientStatus(args.status), limit=args.limit
        )
        items = [
            {
                "id": r["id"],
                "event_id": r["event_id"],
                "school_id": r["school_id"],
                "channel": r["channel"],
                "address": r["address"],
                "attempts": r["attempts"],
                "last_error": r["last_error"],
            }
            for r in rows
        ]
    finally:
        conn.close()
    print(json.dumps(items, ensure_ascii=False, indent=2))
    return 0


def cmd_reinject(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    try:
        ids = reinject_dead(conn, args.recipient_ids, actor=args.actor, note=args.note)
    finally:
        conn.close()
    print(json.dumps({"reinjected": ids}, ensure_ascii=False))
    if not ids:
        return 1
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    sender = LoggingSender(args.spool or _default_spool(args.db))
    worker = DeliveryWorker(
        conn,
        sender,
        renderer=TemplateRenderer(),
        worker_id=args.worker_id,
        batch_size=args.batch_size,
        lease_seconds=args.lease_seconds,
        max_attempts=args.max_attempts,
        base_delay_seconds=args.base_delay,
    )
    print(f"工作进程 {worker.worker_id} 启动，轮询间隔 {args.poll_interval}s", flush=True)
    try:
        while True:
            processed = worker.run_once()
            if processed:
                print(f"处理 {processed} 条", flush=True)
            time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        print("收到中断，退出（租约内消息会在过期后被重新认领）", flush=True)
        return 0
    finally:
        conn.close()


def load_tokens(spec: str | None) -> dict[str, Principal]:
    """令牌配置。优先取 --tokens 文件，其次 OUTBOX_TOKENS 环境变量。

    格式：``{"token-value": {"school_id": 1, "admin": false}}``
    """
    if spec:
        raw = json.loads(Path(spec).read_text(encoding="utf-8"))
    elif os.environ.get("OUTBOX_TOKENS"):
        raw = json.loads(os.environ["OUTBOX_TOKENS"])
    else:
        raise SystemExit("缺少令牌配置：请用 --tokens 指定文件或设置 OUTBOX_TOKENS")
    registry: dict[str, Principal] = {}
    for token, conf in raw.items():
        registry[token] = Principal(
            token=token,
            school_id=conf.get("school_id"),
            is_admin=bool(conf.get("admin", False)),
        )
    return registry


def cmd_serve(args: argparse.Namespace) -> int:
    tokens = load_tokens(args.tokens)
    api = OutboxApi(lambda: connection_scope(args.db), tokens)
    server = serve(api, host=args.host, port=args.port)
    print(f"HTTP 服务监听 {args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="notification_outbox")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=os.environ.get("OUTBOX_DB", "outbox.sqlite3"))
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", parents=[common]).set_defaults(func=cmd_init_db)
    sub.add_parser("backlog", parents=[common]).set_defaults(func=cmd_backlog)

    p = sub.add_parser("list", parents=[common])
    p.add_argument("--status", default="dead", choices=[s.value for s in RecipientStatus])
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_list_dead)

    p = sub.add_parser("reinject", parents=[common])
    p.add_argument("recipient_ids", nargs="+", type=int)
    p.add_argument("--actor", required=True)
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_reinject)

    p = sub.add_parser("worker", parents=[common])
    p.add_argument("--poll-interval", type=float, default=2.0)
    p.add_argument("--batch-size", type=int, default=10)
    p.add_argument("--lease-seconds", type=float, default=30.0)
    p.add_argument("--max-attempts", type=int, default=8)
    p.add_argument("--base-delay", type=float, default=5.0)
    p.add_argument("--worker-id")
    p.add_argument("--spool")
    p.set_defaults(func=cmd_worker)

    p = sub.add_parser("serve", parents=[common])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--tokens")
    p.set_defaults(func=cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
