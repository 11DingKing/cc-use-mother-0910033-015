"""多工作进程并发认领测试：同一条投递不能被两个进程认领/记账。"""
from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from support import FakeClock  # noqa: E402
from notification_outbox.database import connect, initialize, transaction
from notification_outbox.models import Channel, NotificationEvent, Recipient
from notification_outbox.repositories import add_event
from notification_outbox.senders import OutboundMessage
from notification_outbox.worker import DeliveryWorker


class NullSender:
    def send(self, message: OutboundMessage) -> None:
        return None


class ConcurrentClaimTest(unittest.TestCase):
    def test_no_double_claim_across_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "concurrent.sqlite3"
            setup = connect(db_path)
            initialize(setup)
            event = NotificationEvent(
                event_type="batch.approved",
                aggregate_type="schedule_change",
                aggregate_id="B-1",
                school_id=1,
                payload={"n": 50},
                idempotency_key="batch:B-1",
                recipients=tuple(
                    Recipient(Channel.SCHOOL, f"user{i}@school.edu",
                              "schedule_approved_school",
                              {"change_id": f"B-{i}"})
                    for i in range(50)
                ),
            )
            with transaction(setup):
                add_event(setup, event)
            setup.close()

            claimed_by_worker: dict[str, list[int]] = {}
            lock = threading.Lock()

            def run(worker_id: str) -> None:
                conn = connect(db_path)
                worker = DeliveryWorker(
                    conn, NullSender(), worker_id=worker_id,
                    batch_size=7, lease_seconds=30,
                )
                mine: list[int] = []
                while True:
                    rows = worker.claim_batch()
                    if not rows:
                        break
                    mine.extend(r["id"] for r in rows)
                    for row in rows:
                        worker._mark_succeeded(row["id"])
                conn.close()
                with lock:
                    claimed_by_worker[worker_id] = mine

            threads = [
                threading.Thread(target=run, args=(f"w{i}",))
                for i in range(6)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            check = connect(db_path)
            try:
                succeeded = check.execute(
                    "SELECT COUNT(*) AS n FROM outbox_recipient "
                    "WHERE status = 'succeeded'"
                ).fetchone()["n"]
                self.assertEqual(succeeded, 50)
                # 每个接收方只能有一条 success 审计：没有重复记账。
                dupes = check.execute(
                    """
                    SELECT recipient_id, COUNT(*) AS n FROM delivery_attempt
                    WHERE result = 'success'
                    GROUP BY recipient_id HAVING n > 1
                    """
                ).fetchall()
                self.assertEqual(dupes, [])
            finally:
                check.close()

            all_claimed = [
                rid for ids in claimed_by_worker.values() for rid in ids
            ]
            self.assertEqual(len(all_claimed), 50)
            self.assertEqual(len(set(all_claimed)), 50)


if __name__ == "__main__":
    unittest.main()
