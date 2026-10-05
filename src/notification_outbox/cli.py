"""管理命令：查看积压、人工重投、运行工作进程与接口服务。

用法（源码目录下）：
    PYTHONPATH=src python3 -m notification_outbox.cli --db outbox.db backlog
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

from .db import Database
from .sender import LoggingSender
from .service import NotificationService
from .worker import OutboxWorker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="notification-outbox", description="变更通知投递箱管理命令"
    )
    parser.add_argument("--db", default="outbox.db", help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("backlog", help="查看投递积压（事件/接收方计数、最老积压时长）")

    redrive = sub.add_parser("redrive", help="人工重投终止失败的事件")
    redrive.add_argument("event_id")
    redrive.add_argument("--actor", default="ops-cli", help="操作者标识，写入重投审计")
    redrive.add_argument("--reason", required=True, help="重投原因，写入重投审计")

    worker = sub.add_parser("worker", help="运行投递工作进程")
    worker.add_argument("--once", action="store_true", help="处理完当前可认领批次后退出")
    worker.add_argument("--interval", type=float, default=1.0, help="轮询间隔秒数")
    worker.add_argument("--batch-size", type=int, default=10, help="租约批量认领大小")

    serve = sub.add_parser("serve", help="启动 HTTP 接口")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db = Database(args.db)
    service = NotificationService(db)

    if args.command == "backlog":
        print(json.dumps(service.backlog(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    if args.command == "redrive":
        result = service.redrive_event(args.event_id, actor=args.actor, reason=args.reason)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "worker":
        worker = OutboxWorker(db, LoggingSender(), batch_size=args.batch_size)
        if args.once:
            print(json.dumps(worker.run_until_idle(), ensure_ascii=False, sort_keys=True))
            return 0
        while True:
            worker.run_once()
            time.sleep(args.interval)

    if args.command == "serve":
        from .api import create_server

        admin_tokens = {t for t in os.environ.get("OUTBOX_ADMIN_TOKENS", "").split(",") if t}
        tenant_tokens = dict(
            pair.split("=", 1)
            for pair in os.environ.get("OUTBOX_TENANT_TOKENS", "").split(",")
            if pair
        )
        server = create_server(
            args.db,
            tenant_tokens=tenant_tokens,
            admin_tokens=admin_tokens,
            host=args.host,
            port=args.port,
        )
        print(f"监听 http://{args.host}:{args.port}")
        server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
