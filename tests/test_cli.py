"""管理命令测试：积压查看与人工重投。"""
from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from notification_outbox.cli import main
from notification_outbox.db import Database
from notification_outbox.service import ApprovalRequest, NotificationService


class CliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = str(Path(self.tmp.name) / "outbox.db")
        service = NotificationService(Database(self.db_path))
        self.event_id = service.approve_change(
            ApprovalRequest(
                change_id="CHG-1", school_id="school-1", docent_id="docent-7",
                venue_id="venue-3", activity_date="2026-10-12 上午", approved_by="活动统筹员-01",
            )
        ).event_id

    def run_cli(self, *argv: str) -> tuple[int, dict]:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = main(["--db", self.db_path, *argv])
        return code, json.loads(buf.getvalue())

    def test_backlog_command(self) -> None:
        code, report = self.run_cli("backlog")
        self.assertEqual(code, 0)
        self.assertEqual(report["events"]["pending"], 1)
        self.assertEqual(report["deliveries"]["pending"], 3)
        self.assertEqual(report["due_now"], 1)

    def test_worker_once_then_backlog_empty(self) -> None:
        code, report = self.run_cli("worker", "--once")
        self.assertEqual(code, 0)
        self.assertEqual(report["done"], 1)
        _, backlog = self.run_cli("backlog")
        self.assertEqual(backlog["events"]["done"], 1)
        self.assertEqual(backlog["deliveries"]["delivered"], 3)

    def test_redrive_command(self) -> None:
        db = Database(self.db_path)
        with db.transaction() as conn:
            conn.execute(
                "UPDATE outbox_event SET status = 'dead' WHERE event_id = ?", (self.event_id,)
            )
            conn.execute(
                "UPDATE delivery SET status = 'dead' WHERE event_id = ?", (self.event_id,)
            )
        code, result = self.run_cli(
            "redrive", self.event_id, "--actor", "ops", "--reason", "通道恢复后补发"
        )
        self.assertEqual(code, 0)
        self.assertEqual(result["status"], "pending")
        _, backlog = self.run_cli("backlog")
        self.assertEqual(backlog["events"]["pending"], 1)
        self.assertEqual(backlog["redrives_total"], 1)


if __name__ == "__main__":
    unittest.main()
