"""业务服务测试：事务投递箱、幂等批准、重投审计与积压视图。"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from notification_outbox.clock import ManualClock
from notification_outbox.db import Database
from notification_outbox.errors import ConflictError, InvalidStateError
from notification_outbox.service import ApprovalRequest, NotificationService


def make_request(change_id: str = "CHG-001", school_id: str = "school-1") -> ApprovalRequest:
    return ApprovalRequest(
        change_id=change_id,
        school_id=school_id,
        docent_id="docent-7",
        venue_id="venue-3",
        activity_date="2026-10-12 上午",
        approved_by="活动统筹员-01",
        note="三年级研学",
    )


class ServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Database(Path(self.tmp.name) / "outbox.db")
        self.clock = ManualClock()
        self.service = NotificationService(self.db, clock=self.clock)

    def test_approve_writes_change_event_and_deliveries_atomically(self) -> None:
        result = self.service.approve_change(make_request())
        self.assertFalse(result.replayed)
        with self.db.read() as conn:
            change = conn.execute(
                "SELECT * FROM schedule_change WHERE change_id = 'CHG-001'"
            ).fetchone()
            self.assertEqual(change["state"], "已排定")
            event = conn.execute(
                "SELECT * FROM outbox_event WHERE event_id = ?", (result.event_id,)
            ).fetchone()
            self.assertEqual(event["status"], "pending")
            self.assertEqual(event["tenant_id"], "school-1")
            deliveries = conn.execute(
                "SELECT * FROM delivery WHERE event_id = ? ORDER BY recipient_role",
                (result.event_id,),
            ).fetchall()
        self.assertEqual(len(deliveries), 3)
        roles = {d["recipient_role"] for d in deliveries}
        self.assertEqual(roles, {"school_contact", "docent", "venue_admin"})
        # 学校、讲解员、场馆收到的内容各不相同
        bodies = {d["recipient_role"]: json.loads(d["content_json"])["body"] for d in deliveries}
        self.assertEqual(len(set(bodies.values())), 3)
        # 接收方幂等键稳定且唯一
        for d in deliveries:
            self.assertEqual(d["idempotency_key"], f"{result.event_id}:{d['recipient_role']}")

    def test_notification_failure_rolls_back_business_change(self) -> None:
        """通知事件写入失败时，业务变更必须一起回滚，不留半截状态。"""

        def broken_renderer(req: ApprovalRequest):
            raise RuntimeError("模板渲染失败")

        service = NotificationService(self.db, clock=self.clock, renderer=broken_renderer)
        with self.assertRaises(RuntimeError):
            service.approve_change(make_request())
        with self.db.read() as conn:
            for table in ("schedule_change", "outbox_event", "delivery"):
                count = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                self.assertEqual(count, 0, f"{table} 不应残留记录")

    def test_approve_replay_is_idempotent(self) -> None:
        """请求超时重试：同一变更重复批准只产生一条事件、三条投递。"""
        first = self.service.approve_change(make_request())
        second = self.service.approve_change(make_request())
        self.assertTrue(second.replayed)
        self.assertEqual(first.event_id, second.event_id)
        with self.db.read() as conn:
            events = conn.execute("SELECT COUNT(*) AS c FROM outbox_event").fetchone()["c"]
            deliveries = conn.execute("SELECT COUNT(*) AS c FROM delivery").fetchone()["c"]
        self.assertEqual(events, 1)
        self.assertEqual(deliveries, 3)

    def test_approve_conflicting_replay_rejected(self) -> None:
        self.service.approve_change(make_request())
        changed = make_request()
        object.__setattr__(changed, "venue_id", "venue-9")  # frozen dataclass 构造冲突请求
        with self.assertRaises(ConflictError):
            self.service.approve_change(changed)

    def test_tenant_listing_only_contains_own_events(self) -> None:
        self.service.approve_change(make_request("CHG-1", "school-1"))
        self.service.approve_change(make_request("CHG-2", "school-2"))
        mine = self.service.list_tenant_notifications("school-1")
        self.assertEqual(len(mine), 1)
        self.assertEqual(mine[0]["change"]["change_id"], "CHG-1")
        other = self.service.list_tenant_notifications("school-2")
        self.assertEqual(len(other), 1)
        self.assertEqual(other[0]["change"]["change_id"], "CHG-2")

    def test_redrive_requires_dead_status_and_writes_audit(self) -> None:
        result = self.service.approve_change(make_request())
        with self.assertRaises(InvalidStateError):
            self.service.redrive_event(result.event_id, actor="ops", reason="测试")
        with self.db.transaction() as conn:  # 模拟事件已终止失败
            conn.execute(
                "UPDATE outbox_event SET status = 'dead' WHERE event_id = ?",
                (result.event_id,),
            )
            conn.execute(
                "UPDATE delivery SET status = 'dead' WHERE event_id = ?", (result.event_id,)
            )
        out = self.service.redrive_event(result.event_id, actor="ops", reason="通道恢复后补发")
        self.assertEqual(out["status"], "pending")
        with self.db.read() as conn:
            event = conn.execute(
                "SELECT * FROM outbox_event WHERE event_id = ?", (result.event_id,)
            ).fetchone()
            self.assertEqual(event["status"], "pending")
            self.assertEqual(event["attempt_count"], 0)
            pending = conn.execute(
                "SELECT COUNT(*) AS c FROM delivery WHERE event_id = ? AND status = 'pending'",
                (result.event_id,),
            ).fetchone()["c"]
            self.assertEqual(pending, 3)
            audit = conn.execute(
                "SELECT * FROM redrive_audit WHERE event_id = ?", (result.event_id,)
            ).fetchone()
            self.assertEqual(audit["actor"], "ops")
            self.assertEqual(audit["reason"], "通道恢复后补发")
            self.assertEqual(audit["from_status"], "dead")

    def test_redrive_requires_reason(self) -> None:
        result = self.service.approve_change(make_request())
        with self.assertRaises(ValueError):
            self.service.redrive_event(result.event_id, actor="ops", reason="  ")

    def test_backlog_report(self) -> None:
        self.service.approve_change(make_request())
        report = self.service.backlog()
        self.assertEqual(report["events"]["pending"], 1)
        self.assertEqual(report["due_now"], 1)
        self.assertEqual(report["deliveries"]["pending"], 3)
        self.assertEqual(report["oldest_backlog_age_seconds"], 0.0)
        self.clock.advance(5)
        self.assertEqual(self.service.backlog()["oldest_backlog_age_seconds"], 5.0)


if __name__ == "__main__":
    unittest.main()
