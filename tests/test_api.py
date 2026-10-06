"""HTTP API 测试：认证、多租户隔离与管理接口。"""
from __future__ import annotations

import json
import unittest

from support import CHANGE_DATA, FakeClock, make_db, shared_scope  # noqa: E402
from notification_outbox.api import OutboxApi, Principal
from notification_outbox.database import transaction
from notification_outbox.models import RecipientStatus
from notification_outbox.service import approve_change, create_change


SCHOOL_1 = dict(CHANGE_DATA)
SCHOOL_2 = {**CHANGE_DATA, "school_id": 2,
            "school_contact": "school@b.edu",
            "school_name": "第二中学"}

TOKENS = {
    "tok-school-1": Principal("tok-school-1", school_id=1, is_admin=False),
    "tok-school-2": Principal("tok-school-2", school_id=2, is_admin=False),
    "tok-admin": Principal("tok-admin", school_id=None, is_admin=True),
}


class ApiTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.db = make_db()
        self.clock = FakeClock()
        self.api = OutboxApi(lambda: shared_scope(self.db), TOKENS)
        self.auth = {"Authorization": "Bearer tok-school-1"}

    def call(self, method, path, token=None, body=None, headers=None):
        hdrs = dict(headers or {})
        if token is not None:
            hdrs["Authorization"] = f"Bearer {token}"
        raw = json.dumps(body).encode() if body is not None else None
        return self.api.handle(method, path, hdrs, raw)

    def seed(self, data=SCHOOL_1):
        cid = create_change(self.db, data, clock=self.clock)
        with transaction(self.db):
            approve_change(self.db, cid, school_id=data["school_id"], clock=self.clock)
        return cid


class AuthTest(ApiTestBase):
    def test_missing_token_rejected(self) -> None:
        status, body = self.call("GET", "/api/recipients")
        self.assertEqual(status, 401)

    def test_bad_token_rejected(self) -> None:
        status, _ = self.call("GET", "/api/recipients", token="nope")
        self.assertEqual(status, 401)

    def test_school_token_cannot_use_admin(self) -> None:
        status, _ = self.call("GET", "/admin/backlog", token="tok-school-1")
        self.assertEqual(status, 403)

    def test_admin_cannot_use_school_api(self) -> None:
        status, _ = self.call("GET", "/api/recipients", token="tok-admin")
        self.assertEqual(status, 403)

    def test_invalid_json(self) -> None:
        status, body = self.api.handle(
            "POST", "/api/schedule-changes", self.auth, b"{not json"
        )
        self.assertEqual(status, 400)


class TenantIsolationTest(ApiTestBase):
    def test_school_cannot_read_other_school_event(self) -> None:
        cid = self.seed(SCHOOL_2)
        event_id = self.db.execute(
            "SELECT id FROM outbox_event WHERE aggregate_id = ?", (str(cid),)
        ).fetchone()["id"]

        # 学校 1 直接访问学校 2 的事件：404，不承认资源存在。
        status, _ = self.call("GET", f"/api/events/{event_id}", token="tok-school-1")
        self.assertEqual(status, 404)
        # 学校 2 自己可以访问。
        status, body = self.call("GET", f"/api/events/{event_id}", token="tok-school-2")
        self.assertEqual(status, 200)
        self.assertEqual(body["school_id"], 2)

    def test_recipient_list_scoped_to_token_school(self) -> None:
        self.seed(SCHOOL_1)
        self.seed(SCHOOL_2)
        status, body = self.call("GET", "/api/recipients", token="tok-school-1")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["items"]), 3)
        self.assertTrue(all(i["school_id"] == 1 for i in body["items"]))
        # 地址等通知内容只属于本校。
        addresses = {i["address"] for i in body["items"]}
        self.assertEqual(
            addresses, {"school@a.edu", "docent@guide.cn", "venue@museum.cn"}
        )

    def test_cannot_access_other_school_attempts(self) -> None:
        self.seed(SCHOOL_2)
        # 直接查学校 2 的投递项 ID。
        rid = self.db.execute(
            "SELECT r.id FROM outbox_recipient r WHERE r.school_id = 2 "
            "ORDER BY r.id LIMIT 1"
        ).fetchone()["id"]
        status, _ = self.call(
            "GET", f"/api/recipients/{rid}/attempts", token="tok-school-1"
        )
        self.assertEqual(status, 404)

    def test_cannot_create_or_approve_for_other_school(self) -> None:
        # 请求体伪造 school_id 也无效：以令牌为准。
        status, body = self.call(
            "POST", "/api/schedule-changes", token="tok-school-1", body=SCHOOL_2
        )
        self.assertEqual(status, 201)
        created = self.db.execute(
            "SELECT school_id FROM schedule_change WHERE id = ?", (body["id"],)
        ).fetchone()
        self.assertEqual(created["school_id"], 1)

        # 学校 1 批准学校 2 的变更：404。
        cid2 = create_change(self.db, SCHOOL_2, clock=self.clock)
        status, _ = self.call(
            "POST", f"/api/schedule-changes/{cid2}/approve", token="tok-school-1"
        )
        self.assertEqual(status, 404)


class SchoolApiFlowTest(ApiTestBase):
    def test_create_approve_and_view_event(self) -> None:
        body = dict(CHANGE_DATA)
        del body["school_id"]
        status, created = self.call(
            "POST", "/api/schedule-changes", token="tok-school-1", body=body
        )
        self.assertEqual(status, 201)
        cid = created["id"]

        status, approved = self.call(
            "POST", f"/api/schedule-changes/{cid}/approve", token="tok-school-1"
        )
        self.assertEqual(status, 200)
        event_id = approved["event_id"]

        status, event = self.call(
            "GET", f"/api/events/{event_id}", token="tok-school-1"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(event["recipients"]), 3)
        channels = {r["channel"] for r in event["recipients"]}
        self.assertEqual(channels, {"school", "docent", "venue"})

        # 重复批准幂等，事件 ID 不变。
        status, again = self.call(
            "POST", f"/api/schedule-changes/{cid}/approve", token="tok-school-1"
        )
        self.assertEqual(status, 200)
        self.assertEqual(again["event_id"], event_id)

    def test_approve_unknown_change_404(self) -> None:
        status, _ = self.call(
            "POST", "/api/schedule-changes/999/approve", token="tok-school-1"
        )
        self.assertEqual(status, 404)

    def test_filter_recipients_by_status(self) -> None:
        self.seed()
        status, body = self.call(
            "GET", "/api/recipients?status=pending", token="tok-school-1"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["items"]), 3)
        status, body = self.call(
            "GET", f"/api/recipients?status={RecipientStatus.SUCCEEDED.value}",
            token="tok-school-1",
        )
        self.assertEqual(len(body["items"]), 0)

    def test_bad_status_returns_400(self) -> None:
        self.seed()
        status, _ = self.call(
            "GET", "/api/recipients?status=bogus", token="tok-school-1"
        )
        self.assertEqual(status, 400)


class AdminApiTest(ApiTestBase):
    def test_backlog_is_cross_school(self) -> None:
        self.seed(SCHOOL_1)
        self.seed(SCHOOL_2)
        status, body = self.call("GET", "/admin/backlog", token="tok-admin")
        self.assertEqual(status, 200)
        self.assertEqual(body["pending"], 6)
        self.assertEqual(body["total"], 6)

    def test_admin_reinject_flow(self) -> None:
        cid = self.seed(SCHOOL_1)
        # 将场馆投递项直接置为 dead，模拟终止失败。
        rid = self.db.execute(
            "SELECT r.id FROM outbox_recipient r "
            "JOIN outbox_event e ON e.id = r.event_id "
            "WHERE e.aggregate_id = ? AND r.channel = 'venue'",
            (str(cid),),
        ).fetchone()["id"]
        self.db.execute(
            "UPDATE outbox_recipient SET status='dead' WHERE id = ?", (rid,)
        )
        status, body = self.call(
            "POST", "/admin/recipients/reinject", token="tok-admin",
            body={"recipient_ids": [rid], "note": "地址已更正"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["reinjected"], [rid])

        action = self.db.execute(
            "SELECT actor, note FROM outbox_admin_action WHERE recipient_id = ?",
            (rid,),
        ).fetchone()
        self.assertEqual(action["actor"], "tok-admin")
        self.assertEqual(action["note"], "地址已更正")

    def test_reinject_validates_input(self) -> None:
        status, _ = self.call(
            "POST", "/admin/recipients/reinject", token="tok-admin",
            body={"recipient_ids": []},
        )
        self.assertEqual(status, 400)
        status, _ = self.call(
            "POST", "/admin/recipients/reinject", token="tok-admin",
            body={"recipient_ids": "x"},
        )
        self.assertEqual(status, 400)

    def test_unknown_route_404(self) -> None:
        status, _ = self.call("GET", "/nope", token="tok-school-1")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
