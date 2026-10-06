"""同事务写入与幂等键测试。"""
from __future__ import annotations

import json
import unittest

from support import CHANGE_DATA, FakeClock, make_db  # noqa: E402
from notification_outbox.database import transaction
from notification_outbox.models import Channel, NotificationEvent, Recipient
from notification_outbox.repositories import add_event
from notification_outbox.service import (
    BusinessError,
    approve_change,
    create_change,
)


class TransactionalWriteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = make_db()
        self.clock = FakeClock()

    def test_approve_writes_business_and_events_atomically(self) -> None:
        cid = create_change(self.conn, CHANGE_DATA, clock=self.clock)
        with transaction(self.conn):
            event_id = approve_change(self.conn, cid, school_id=1, clock=self.clock)

        event = self.conn.execute(
            "SELECT * FROM outbox_event WHERE id = ?", (event_id,)
        ).fetchone()
        self.assertEqual(event["event_type"], "schedule_change.approved")
        recipients = self.conn.execute(
            "SELECT channel, address, template FROM outbox_recipient ORDER BY id"
        ).fetchall()
        self.assertEqual([r["channel"] for r in recipients],
                         ["school", "docent", "venue"])
        self.assertEqual([r["address"] for r in recipients],
                         ["school@a.edu", "docent@guide.cn", "venue@museum.cn"])
        status = self.conn.execute(
            "SELECT status FROM schedule_change WHERE id = ?", (cid,)
        ).fetchone()["status"]
        self.assertEqual(status, "已排定")

    def test_business_failure_rolls_back_notifications(self) -> None:
        with self.assertRaises(BusinessError):
            with transaction(self.conn):
                approve_change(self.conn, 999, school_id=1, clock=self.clock)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) AS n FROM outbox_event").fetchone()["n"],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM outbox_recipient"
            ).fetchone()["n"],
            0,
        )

    def test_notification_write_failure_rolls_back_business_approval(self) -> None:
        cid = create_change(self.conn, CHANGE_DATA, clock=self.clock)

        # 破坏接收方表，让通知写入失败；业务批准必须一起回滚。
        self.conn.execute("DROP TABLE outbox_recipient")
        with self.assertRaises(Exception):
            with transaction(self.conn):
                approve_change(self.conn, cid, school_id=1, clock=self.clock)

        status = self.conn.execute(
            "SELECT status FROM schedule_change WHERE id = ?", (cid,)
        ).fetchone()["status"]
        self.assertEqual(status, "待确认")
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) AS n FROM outbox_event").fetchone()["n"],
            0,
        )

    def test_retried_request_does_not_duplicate_event(self) -> None:
        cid = create_change(self.conn, CHANGE_DATA, clock=self.clock)
        with transaction(self.conn):
            e1 = approve_change(self.conn, cid, school_id=1, clock=self.clock)
        # 客户端超时后原样重试：同一业务单号 -> 同一事件，无重复接收方。
        with transaction(self.conn):
            e2 = approve_change(self.conn, cid, school_id=1, clock=self.clock)
        self.assertEqual(e1, e2)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM outbox_event"
            ).fetchone()["n"],
            1,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM outbox_recipient"
            ).fetchone()["n"],
            3,
        )

    def test_three_channels_get_different_rendered_content(self) -> None:
        from notification_outbox.rendering import TemplateRenderer

        cid = create_change(self.conn, CHANGE_DATA, clock=self.clock)
        with transaction(self.conn):
            approve_change(self.conn, cid, school_id=1, clock=self.clock)
        renderer = TemplateRenderer()
        rows = self.conn.execute(
            "SELECT template, params FROM outbox_recipient ORDER BY id"
        ).fetchall()
        rendered = {
            row["template"]: renderer.render(row["template"], json.loads(row["params"]))
            for row in rows
        }
        subjects = {s for s, _ in rendered.values()}
        self.assertEqual(len(subjects), 3)
        body_school = rendered["schedule_approved_school"][1]
        body_venue = rendered["schedule_approved_venue"][1]
        self.assertIn("贵单位申请", body_school)
        self.assertIn("请预留场地", body_venue)

    def test_add_event_rejects_duplicate_recipient_in_event(self) -> None:
        with self.assertRaises(ValueError):
            NotificationEvent(
                event_type="t", aggregate_type="a", aggregate_id="1",
                school_id=1, payload={}, idempotency_key="k",
                recipients=(
                    Recipient(Channel.SCHOOL, "x@x", "tpl"),
                    Recipient(Channel.SCHOOL, "x@x", "tpl"),
                ),
            )


if __name__ == "__main__":
    unittest.main()
