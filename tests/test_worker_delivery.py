"""租约认领、退避重试、终止失败与重启恢复测试。"""
from __future__ import annotations

import unittest

from support import (  # noqa: E402
    CHANGE_DATA, FakeClock, FixedRandom, ScriptedSender, make_db,
)
from notification_outbox.clock import parse_text
from notification_outbox.database import transaction
from notification_outbox.repositories import backlog_summary
from notification_outbox.service import approve_change, create_change
from notification_outbox.worker import DeliveryWorker, reinject_dead


def seed_approved(conn, clock) -> tuple[int, int]:
    cid = create_change(conn, CHANGE_DATA, clock=clock)
    with transaction(conn):
        event_id = approve_change(conn, cid, school_id=1, clock=clock)
    return cid, event_id


def make_worker(conn, sender, clock, **kwargs):
    kwargs.setdefault("base_delay_seconds", 10.0)
    kwargs.setdefault("max_attempts", 4)
    kwargs.setdefault("lease_seconds", 30.0)
    kwargs.setdefault("rng", FixedRandom())
    kwargs.setdefault("worker_id", "w1")
    return DeliveryWorker(conn, sender, clock=clock, **kwargs)


class LeaseClaimTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = make_db()
        self.clock = FakeClock()
        self.sender = ScriptedSender()
        seed_approved(self.conn, self.clock)

    def test_batch_claim_and_success(self) -> None:
        worker = make_worker(self.conn, self.sender, self.clock)
        n = worker.run_once()
        self.assertEqual(n, 3)
        rows = self.conn.execute(
            "SELECT status, attempts, lease_owner, leased_until "
            "FROM outbox_recipient ORDER BY id"
        ).fetchall()
        self.assertTrue(all(r["status"] == "succeeded" for r in rows))
        self.assertTrue(all(r["attempts"] == 1 for r in rows))
        self.assertTrue(all(r["lease_owner"] is None for r in rows))
        attempts = self.conn.execute(
            "SELECT result, COUNT(*) AS n FROM delivery_attempt GROUP BY result"
        ).fetchall()
        self.assertEqual({r["result"]: r["n"] for r in attempts}, {"success": 3})
        self.assertEqual(len(self.sender.calls), 3)
        # 每条消息带稳定去重键。
        keys = {m.dedupe_key for m in self.sender.calls}
        self.assertEqual(len(keys), 3)
        self.assertTrue(all(k.startswith("schedule-change-approved:1:") for k in keys))

    def test_claimed_rows_are_not_claimed_again_while_lease_valid(self) -> None:
        worker = make_worker(self.conn, self.sender, self.clock)
        claimed = worker.claim_batch()
        self.assertEqual(len(claimed), 3)
        # 租约有效期内另一个进程认领不到。
        other = DeliveryWorker(
            self.conn, self.sender, worker_id="w2",
            clock=self.clock, rng=FixedRandom(),
        )
        self.assertEqual(other.claim_batch(), [])
        # 即使原进程崩溃（没有写结果），重启后的进程在租约过期后能回收。
        self.clock.advance(31)
        recovered = other.claim_batch()
        self.assertEqual(len(recovered), 3)
        self.assertTrue(all(r["lease_owner"] == "w2" for r in recovered))

    def test_empty_queue_returns_zero(self) -> None:
        worker = make_worker(self.conn, self.sender, self.clock)
        self.assertEqual(worker.run_once(), 3)
        self.clock.advance(1)
        self.assertEqual(worker.run_once(), 0)


class BackoffRetryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = make_db()
        self.clock = FakeClock()
        seed_approved(self.conn, self.clock)

    def _key(self, channel: str) -> str:
        return f"schedule-change-approved:1:{channel}:{CHANGE_DATA[f'{channel}_contact']}"

    def test_transient_failure_backs_off_then_succeeds(self) -> None:
        # 讲解员渠道前两次超时，第三次成功；其他渠道一次成功。
        sender = ScriptedSender({
            self._key("docent"): [
                ("transient", "timeout"),
                ("transient", "bad_gateway"),
            ],
        })
        worker = make_worker(self.conn, self.sender_with(sender), self.clock)

        self.assertEqual(worker.run_once(), 3)
        row = self.conn.execute(
            "SELECT * FROM outbox_recipient WHERE channel = 'docent'"
        ).fetchone()
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["attempts"], 1)
        not_before = parse_text(row["not_before"])
        self.assertGreater(not_before, self.clock.now())
        # base=10s，第一次退避 10 * 2^0 * 抖动(1.0) = 10s
        self.assertEqual(
            (not_before - self.clock.now()).total_seconds(), 10.0
        )
        self.assertIn("timeout", row["last_error"])

        # 未到退避时间不会被认领。
        self.clock.advance(9)
        self.assertEqual(worker.run_once(), 0)
        self.clock.advance(2)
        self.assertEqual(worker.run_once(), 1)  # 第二次仍失败
        row = self.conn.execute(
            "SELECT attempts, not_before FROM outbox_recipient WHERE channel='docent'"
        ).fetchone()
        self.assertEqual(row["attempts"], 2)
        # 第二次退避 10 * 2^1 = 20s
        self.assertAlmostEqual(
            (parse_text(row["not_before"]) - self.clock.now()).total_seconds(),
            20.0,
        )

        self.clock.advance(21)
        self.assertEqual(worker.run_once(), 1)  # 第三次成功
        row = self.conn.execute(
            "SELECT status, attempts FROM outbox_recipient WHERE channel='docent'"
        ).fetchone()
        self.assertEqual(row["status"], "succeeded")
        self.assertEqual(row["attempts"], 3)

        failures = self.conn.execute(
            "SELECT attempt_no, error_code, result FROM delivery_attempt "
            "JOIN outbox_recipient r ON r.id = recipient_id "
            "WHERE r.channel = 'docent' ORDER BY delivery_attempt.id"
        ).fetchall()
        self.assertEqual(
            [(a["attempt_no"], a["error_code"], a["result"]) for a in failures],
            [(1, "timeout", "failure"),
             (2, "bad_gateway", "failure"),
             (3, None, "success")],
        )

    def sender_with(self, sender):
        return sender

    def test_max_attempts_marks_dead(self) -> None:
        key = self._key("venue")
        sender = ScriptedSender({
            key: [("transient", "timeout")] * 10,
        })
        worker = make_worker(self.conn, sender, self.clock, max_attempts=3)
        worker.run_once()  # attempt 1 -> backoff
        for attempt in range(2, 4):
            row = self.conn.execute(
                "SELECT not_before FROM outbox_recipient WHERE channel='venue'"
            ).fetchone()
            self.clock.now_value = parse_text(row["not_before"])
            worker.run_once()
        row = self.conn.execute(
            "SELECT * FROM outbox_recipient WHERE channel='venue'"
        ).fetchone()
        self.assertEqual(row["status"], "dead")
        self.assertEqual(row["attempts"], 3)
        self.assertIsNotNone(row["dead_at"])
        final = self.conn.execute(
            "SELECT result FROM delivery_attempt "
            "JOIN outbox_recipient r ON r.id = recipient_id "
            "WHERE r.channel='venue' ORDER BY delivery_attempt.id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(final["result"], "dead")

        # dead 之后不会再被认领。
        self.clock.advance(10_000)
        self.assertEqual(worker.run_once(), 0)

    def test_permanent_error_goes_dead_immediately(self) -> None:
        key = self._key("school")
        sender = ScriptedSender({key: [("permanent", "address_not_found")]})
        worker = make_worker(self.conn, sender, self.clock)
        worker.run_once()
        row = self.conn.execute(
            "SELECT * FROM outbox_recipient WHERE channel='school'"
        ).fetchone()
        self.assertEqual(row["status"], "dead")
        self.assertEqual(row["attempts"], 1)
        self.assertIn("address_not_found", row["last_error"])

    def test_unexpected_exception_is_treated_as_transient(self) -> None:
        class Boom:
            def __init__(self):
                self.calls = 0

            def send(self, message) -> None:
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("通道库内部错误")

        boom = Boom()
        worker = make_worker(self.conn, boom, self.clock)
        worker.run_once()
        row = self.conn.execute(
            "SELECT status, last_error FROM outbox_recipient "
            "WHERE channel='school'"
        ).fetchone()
        self.assertEqual(row["status"], "pending")
        self.assertIn("unexpected_error", row["last_error"])


class RestartAndReinjectTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = make_db()
        self.clock = FakeClock()
        seed_approved(self.conn, self.clock)

    def test_crash_after_claim_recovers_after_restart(self) -> None:
        sender = ScriptedSender()
        w1 = make_worker(self.conn, sender, self.clock, worker_id="w1")
        w1.claim_batch()
        # 模拟进程被 kill -9：结果从未落盘。
        del w1

        sender2 = ScriptedSender()
        w2 = DeliveryWorker(
            self.conn, sender2, worker_id="w2",
            clock=self.clock, rng=FixedRandom(), lease_seconds=30,
        )
        self.assertEqual(w2.claim_batch(), [])  # 租约未过期
        self.clock.advance(31)
        n = w2.run_once()
        self.assertEqual(n, 3)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM outbox_recipient WHERE status='succeeded'"
            ).fetchone()["n"],
            3,
        )

    def test_reinject_dead_requeues_and_keeps_audit(self) -> None:
        key = "schedule-change-approved:1:venue:venue@museum.cn"
        sender = ScriptedSender({key: [("permanent", "bad_address")]})
        worker = make_worker(self.conn, sender, self.clock)
        worker.run_once()
        rid = self.conn.execute(
            "SELECT id FROM outbox_recipient WHERE channel='venue'"
        ).fetchone()["id"]

        # 非 dead 的不能被重投。
        other = self.conn.execute(
            "SELECT id FROM outbox_recipient WHERE channel='school'"
        ).fetchone()["id"]
        result = reinject_dead(
            self.conn, [rid, other], actor="admin-li", clock=self.clock
        )
        self.assertEqual(result, [rid])

        row = self.conn.execute(
            "SELECT * FROM outbox_recipient WHERE id = ?", (rid,)
        ).fetchone()
        self.assertEqual(row["status"], "reopened")
        self.assertEqual(row["attempts"], 0)
        self.assertIsNone(row["dead_at"])

        action = self.conn.execute(
            "SELECT action, actor FROM outbox_admin_action WHERE recipient_id = ?",
            (rid,),
        ).fetchone()
        self.assertEqual(action["action"], "reinject")
        self.assertEqual(action["actor"], "admin-li")

        # 历史失败审计仍保留。
        history = self.conn.execute(
            "SELECT COUNT(*) AS n FROM delivery_attempt WHERE recipient_id = ?",
            (rid,),
        ).fetchone()["n"]
        self.assertGreaterEqual(history, 1)

        # 重投后被重新投递，dedupe_key 不变（通道侧可据此不重复入账）。
        sender2 = ScriptedSender()
        worker2 = make_worker(self.conn, sender2, self.clock, worker_id="w9")
        self.clock.advance(1)
        worker2.run_once()
        keys = [m.dedupe_key for m in sender2.calls if m.recipient_id == rid]
        self.assertEqual(keys, [key])
        row = self.conn.execute(
            "SELECT status FROM outbox_recipient WHERE id = ?", (rid,)
        ).fetchone()
        self.assertEqual(row["status"], "succeeded")

    def test_reinject_empty_is_noop(self) -> None:
        self.assertEqual(reinject_dead(self.conn, [], actor="x"), [])


class BacklogTest(unittest.TestCase):
    def test_summary_counts(self) -> None:
        conn = make_db()
        clock = FakeClock()
        seed_approved(conn, clock)
        summary = backlog_summary(conn)
        self.assertEqual(summary["pending"], 3)
        self.assertEqual(summary["due_now"], 3)
        self.assertEqual(summary["succeeded"], 0)
        worker = make_worker(conn, ScriptedSender(), clock)
        worker.run_once()
        summary = backlog_summary(conn)
        self.assertEqual(summary["succeeded"], 3)
        self.assertEqual(summary["due_now"], 0)


if __name__ == "__main__":
    unittest.main()
