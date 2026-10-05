"""工作进程测试：租约认领、幂等去重、退避重试、终止失败与重启恢复。"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from notification_outbox.clock import ManualClock, from_iso
from notification_outbox.db import Database
from notification_outbox.errors import PermanentSendError, TransientSendError
from notification_outbox.sender import RecordingSender
from notification_outbox.service import ApprovalRequest, NotificationService
from notification_outbox.worker import OutboxWorker


class TimeoutAfterFirstSend:
    """首次发送实际成功但客户端看到超时：重试时必须按幂等键去重。"""

    def __init__(self, inner: RecordingSender):
        self._inner = inner
        self._timed_out: set[str] = set()

    def send(self, *, idempotency_key, recipient_role, recipient_id, content):
        if idempotency_key not in self._timed_out:
            self._timed_out.add(idempotency_key)
            self._inner.send(
                idempotency_key=idempotency_key,
                recipient_role=recipient_role,
                recipient_id=recipient_id,
                content=content,
            )
            raise TransientSendError("等待通道确认超时")
        return self._inner.send(
            idempotency_key=idempotency_key,
            recipient_role=recipient_role,
            recipient_id=recipient_id,
            content=content,
        )


class RoleFailingSender:
    """按接收方角色注入失败的通道。"""

    def __init__(self, inner, *, transient_roles=(), permanent_roles=()):
        self._inner = inner
        self._transient = set(transient_roles)
        self._permanent = set(permanent_roles)

    def send(self, *, idempotency_key, recipient_role, recipient_id, content):
        if recipient_role in self._permanent:
            raise PermanentSendError("接收方不存在")
        if recipient_role in self._transient:
            raise TransientSendError("通道暂时不可用")
        return self._inner.send(
            idempotency_key=idempotency_key,
            recipient_role=recipient_role,
            recipient_id=recipient_id,
            content=content,
        )


class WorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Database(Path(self.tmp.name) / "outbox.db")
        self.clock = ManualClock()
        self.service = NotificationService(self.db, clock=self.clock)
        self.sender = RecordingSender()
        self.worker = OutboxWorker(
            self.db,
            self.sender,
            clock=self.clock,
            worker_id="w1",
            lease_seconds=30,
            base_backoff_seconds=10,
            max_backoff_seconds=300,
        )

    def approve(self, change_id: str = "CHG-1", school: str = "school-1"):
        return self.service.approve_change(
            ApprovalRequest(
                change_id=change_id,
                school_id=school,
                docent_id="docent-7",
                venue_id="venue-3",
                activity_date="2026-10-12 上午",
                approved_by="活动统筹员-01",
            )
        )

    def event_row(self, event_id: str):
        with self.db.read() as conn:
            return conn.execute(
                "SELECT * FROM outbox_event WHERE event_id = ?", (event_id,)
            ).fetchone()

    def delivery_rows(self, event_id: str):
        with self.db.read() as conn:
            return conn.execute(
                "SELECT * FROM delivery WHERE event_id = ? ORDER BY recipient_role",
                (event_id,),
            ).fetchall()

    def test_delivers_all_recipients_and_completes(self) -> None:
        result = self.approve()
        report = self.worker.run_until_idle()
        self.assertEqual(report["done"], 1)
        self.assertEqual(self.event_row(result.event_id)["status"], "done")
        # 三个接收方各自收到内容不同的消息
        self.assertEqual(len(self.sender.messages), 3)
        roles = {m["recipient_role"] for m in self.sender.messages.values()}
        self.assertEqual(roles, {"school_contact", "docent", "venue_admin"})
        for dlv in self.delivery_rows(result.event_id):
            self.assertEqual(dlv["status"], "delivered")
            self.assertIsNotNone(dlv["delivered_at"])
        with self.db.read() as conn:
            attempts = conn.execute(
                "SELECT * FROM delivery_attempt WHERE event_id = ?", (result.event_id,)
            ).fetchall()
        self.assertEqual(len(attempts), 3)
        self.assertTrue(all(a["outcome"] == "delivered" for a in attempts))
        self.assertTrue(all(a["worker_id"] == "w1" for a in attempts))

    def test_lease_batch_claim_respects_batch_size(self) -> None:
        ids = [self.approve(f"CHG-{i}").event_id for i in range(3)]
        worker = OutboxWorker(
            self.db, self.sender, clock=self.clock, worker_id="w1",
            batch_size=2, lease_seconds=30, base_backoff_seconds=10,
        )
        first = worker.claim_batch()
        self.assertEqual(len(first), 2)
        second = worker.claim_batch()
        self.assertEqual(len(second), 1)  # 剩余一条待认领
        self.assertEqual(worker.claim_batch(), [])  # 全部在租约内
        for event_id in ids:
            row = self.event_row(event_id)
            self.assertEqual(row["status"], "processing")
            self.assertEqual(row["claimed_by"], "w1")
            # 租约到期时间 = 当前时间 + lease_seconds
            self.assertEqual(
                (from_iso(row["claimed_until"]) - self.clock.now()).total_seconds(), 30
            )
        # 处理完租下的事件后事件完成
        for event_id in ids:
            self.assertEqual(worker.process_event(event_id), "done")

    def test_lease_expiry_allows_reclaim_after_restart(self) -> None:
        """工作进程崩溃/服务重启：租约到期后事件被新进程接管，不丢消息。"""
        result = self.approve()
        crashed = OutboxWorker(
            self.db, self.sender, clock=self.clock, worker_id="crashed",
            lease_seconds=30, base_backoff_seconds=10,
        )
        self.assertEqual(crashed.claim_batch(), [result.event_id])
        # crashed 进程宕机，未处理任何事件
        survivor = OutboxWorker(
            self.db, self.sender, clock=self.clock, worker_id="survivor",
            lease_seconds=30, base_backoff_seconds=10,
        )
        self.assertEqual(survivor.claim_batch(), [])  # 租约未到期，不能抢占
        self.clock.advance(31)  # 租约到期
        report = survivor.run_until_idle()
        self.assertEqual(report["done"], 1)
        self.assertEqual(self.event_row(result.event_id)["status"], "done")
        self.assertEqual(len(self.sender.messages), 3)

    def test_transient_failure_retries_with_exponential_backoff(self) -> None:
        failing = RoleFailingSender(self.sender, transient_roles={"docent", "school_contact", "venue_admin"})
        worker = OutboxWorker(
            self.db, failing, clock=self.clock, worker_id="w1",
            lease_seconds=30, base_backoff_seconds=10, max_backoff_seconds=300,
        )
        result = self.approve()

        self.assertEqual(worker.run_once()["retry"], 1)
        row = self.event_row(result.event_id)
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["attempt_count"], 1)
        self.assertEqual((from_iso(row["next_attempt_at"]) - self.clock.now()).total_seconds(), 10)

        self.assertEqual(worker.claim_batch(), [])  # 未到重试时间
        self.clock.advance(10)
        self.assertEqual(worker.run_once()["retry"], 1)
        row = self.event_row(result.event_id)
        self.assertEqual(row["attempt_count"], 2)
        self.assertEqual((from_iso(row["next_attempt_at"]) - self.clock.now()).total_seconds(), 20)

        self.clock.advance(20)
        self.assertEqual(worker.run_once()["retry"], 1)
        row = self.event_row(result.event_id)
        self.assertEqual((from_iso(row["next_attempt_at"]) - self.clock.now()).total_seconds(), 40)

    def test_dead_after_max_attempts(self) -> None:
        service = NotificationService(self.db, clock=self.clock, max_attempts=3)
        failing = RoleFailingSender(self.sender, transient_roles={"docent", "school_contact", "venue_admin"})
        worker = OutboxWorker(
            self.db, failing, clock=self.clock, worker_id="w1",
            lease_seconds=30, base_backoff_seconds=10,
        )
        result = service.approve_change(
            ApprovalRequest(
                change_id="CHG-1", school_id="school-1", docent_id="docent-7",
                venue_id="venue-3", activity_date="2026-10-12 上午", approved_by="活动统筹员-01",
            )
        )
        self.assertEqual(worker.run_once()["retry"], 1)
        self.clock.advance(10)
        self.assertEqual(worker.run_once()["retry"], 1)
        self.clock.advance(20)
        self.assertEqual(worker.run_once()["dead"], 1)  # 第三次处理达到上限，终止失败
        row = self.event_row(result.event_id)
        self.assertEqual(row["status"], "dead")
        for dlv in self.delivery_rows(result.event_id):
            self.assertEqual(dlv["status"], "dead")
            self.assertIn("超过最大重试次数", dlv["last_error"])
        backlog = self.service.backlog()
        self.assertEqual(backlog["events"]["dead"], 1)
        self.assertEqual(backlog["deliveries"]["dead"], 3)

    def test_duplicate_delivery_is_deduped(self) -> None:
        """已送达但结果未记录（超时）：重试按幂等键去重，接收方只收到一条消息。"""
        flaky = TimeoutAfterFirstSend(self.sender)
        worker = OutboxWorker(
            self.db, flaky, clock=self.clock, worker_id="w1",
            lease_seconds=30, base_backoff_seconds=10,
        )
        result = self.approve()
        self.assertEqual(worker.run_once()["retry"], 1)  # 三次发送都「超时」，但消息实际已送达
        self.assertEqual(len(self.sender.messages), 3)

        self.clock.advance(10)
        report = worker.run_once()
        self.assertEqual(report["done"], 1)
        self.assertEqual(self.event_row(result.event_id)["status"], "done")
        # 通道侧没有产生任何重复消息
        self.assertEqual(len(self.sender.messages), 3)
        with self.db.read() as conn:
            outcomes = [
                row["outcome"]
                for row in conn.execute(
                    "SELECT outcome FROM delivery_attempt WHERE event_id = ? ORDER BY attempt_id",
                    (result.event_id,),
                ).fetchall()
            ]
        self.assertEqual(outcomes.count("failed"), 3)
        self.assertEqual(outcomes.count("duplicate"), 3)
        for dlv in self.delivery_rows(result.event_id):
            self.assertEqual(dlv["status"], "delivered")

    def test_permanent_failure_marks_recipient_dead_then_redrive_heals(self) -> None:
        """单个接收方终止失败不影响其他接收方；修复通道后人工重投补发。"""
        failing = RoleFailingSender(self.sender, permanent_roles={"venue_admin"})
        worker = OutboxWorker(
            self.db, failing, clock=self.clock, worker_id="w1",
            lease_seconds=30, base_backoff_seconds=10,
        )
        result = self.approve()
        self.assertEqual(worker.run_once()["dead"], 1)
        deliveries = {d["recipient_role"]: d for d in self.delivery_rows(result.event_id)}
        self.assertEqual(deliveries["school_contact"]["status"], "delivered")
        self.assertEqual(deliveries["docent"]["status"], "delivered")
        self.assertEqual(deliveries["venue_admin"]["status"], "dead")
        self.assertIn("permanent", deliveries["venue_admin"]["last_error"])
        self.assertEqual(len(self.sender.messages), 2)  # 场馆未收到消息

        # 通道修复后人工重投：只补发终止失败的接收方
        healthy_worker = OutboxWorker(
            self.db, self.sender, clock=self.clock, worker_id="w2",
            lease_seconds=30, base_backoff_seconds=10,
        )
        self.service.redrive_event(result.event_id, actor="ops", reason="场馆通道恢复")
        self.assertEqual(healthy_worker.run_until_idle()["done"], 1)
        self.assertEqual(self.event_row(result.event_id)["status"], "done")
        self.assertEqual(len(self.sender.messages), 3)
        with self.db.read() as conn:
            audit = conn.execute(
                "SELECT * FROM redrive_audit WHERE event_id = ?", (result.event_id,)
            ).fetchone()
        self.assertEqual(audit["actor"], "ops")

    def test_run_until_idle_processes_multiple_batches(self) -> None:
        for i in range(3):
            self.approve(f"CHG-{i}")
        worker = OutboxWorker(
            self.db, self.sender, clock=self.clock, worker_id="w1",
            batch_size=1, lease_seconds=30, base_backoff_seconds=10,
        )
        report = worker.run_until_idle()
        self.assertEqual(report["done"], 3)
        self.assertEqual(len(self.sender.messages), 9)


if __name__ == "__main__":
    unittest.main()
