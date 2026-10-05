"""业务服务：批准排班变更并同事务写入投递箱；查询、积压视图与人工重投。

不变量「事务投递箱」在此落实：schedule_change、outbox_event、delivery 三张表
在同一个 SQLite 事务中写入，任何一步失败都会整体回滚，杜绝「数据库更新成功但
通知事件丢失」的半截状态。
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from .clock import Clock, SystemClock, from_iso, to_iso
from .db import Database
from .errors import ConflictError, InvalidStateError, NotFoundError

EVENT_SCHEDULE_APPROVED = "schedule_change.approved"
STATE_APPROVED = "已排定"
DEFAULT_MAX_ATTEMPTS = 8

# 接收方角色 -> 领域契约中的参与者
RECIPIENT_ROLES = {
    "school_contact": "学校联系人",
    "docent": "讲解员",
    "venue_admin": "场馆管理员",
}


@dataclass(frozen=True)
class ApprovalRequest:
    change_id: str
    school_id: str
    docent_id: str
    venue_id: str
    activity_date: str
    approved_by: str
    note: str = ""


@dataclass(frozen=True)
class ApprovalResult:
    change_id: str
    event_id: str
    state: str
    replayed: bool  # True 表示命中幂等重放，未产生新事件


def render_notifications(req: ApprovalRequest) -> list[tuple[str, str, dict]]:
    """按接收方生成不同内容：学校联系人、讲解员、场馆管理员。"""
    return [
        (
            "school_contact",
            req.school_id,
            {
                "template": "schedule_approved.school",
                "title": "参观活动已排定",
                "body": (
                    f"您校预约的参观活动已排定：{req.activity_date}，"
                    f"场馆 {req.venue_id}，讲解员 {req.docent_id}。请组织师生按时到场。"
                ),
            },
        ),
        (
            "docent",
            req.docent_id,
            {
                "template": "schedule_approved.docent",
                "title": "新讲解任务",
                "body": (
                    f"您有新的讲解任务：{req.activity_date}，场馆 {req.venue_id}，"
                    f"学校 {req.school_id}。请提前确认讲解物料。"
                ),
            },
        ),
        (
            "venue_admin",
            req.venue_id,
            {
                "template": "schedule_approved.venue",
                "title": "场馆排期确认",
                "body": (
                    f"场馆新增已排定活动：{req.activity_date}，学校 {req.school_id}，"
                    f"讲解员 {req.docent_id}。请预留场地与设备。"
                ),
            },
        ),
    ]


class NotificationService:
    def __init__(
        self,
        db: Database,
        *,
        clock: Clock = SystemClock(),
        renderer=render_notifications,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        self._db = db
        self._clock = clock
        self._renderer = renderer
        self._max_attempts = max_attempts

    def approve_change(self, req: ApprovalRequest) -> ApprovalResult:
        """批准排班变更：业务状态、投递箱事件、各接收方投递记录同事务落库。

        请求超时重试时，(event_type, aggregate_id) 唯一约束保证只存在一条事件，
        重放请求幂等返回原事件，不产生重复通知。
        """
        now = to_iso(self._clock.now())
        with self._db.transaction() as conn:
            existing = conn.execute(
                "SELECT event_id, payload_json FROM outbox_event"
                " WHERE event_type = ? AND aggregate_id = ?",
                (EVENT_SCHEDULE_APPROVED, req.change_id),
            ).fetchone()
            if existing is not None:
                change = json.loads(existing["payload_json"])["change"]
                for field in ("school_id", "docent_id", "venue_id", "activity_date"):
                    if change[field] != getattr(req, field):
                        raise ConflictError(
                            f"变更 {req.change_id} 已批准，重放请求的 {field} 与原件不一致"
                        )
                return ApprovalResult(req.change_id, existing["event_id"], STATE_APPROVED, True)

            # 1) 业务变更：排班状态推进为「已排定」
            conn.execute(
                """INSERT INTO schedule_change
                       (change_id, school_id, docent_id, venue_id, activity_date,
                        state, approved_by, approved_at, note, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(change_id) DO UPDATE SET
                       state = excluded.state,
                       approved_by = excluded.approved_by,
                       approved_at = excluded.approved_at,
                       note = excluded.note,
                       updated_at = excluded.updated_at""",
                (
                    req.change_id, req.school_id, req.docent_id, req.venue_id,
                    req.activity_date, STATE_APPROVED, req.approved_by, now, req.note, now,
                ),
            )

            # 2) 同事务写入投递箱事件
            event_id = f"evt_{uuid.uuid4().hex}"
            payload = {
                "change": {
                    "change_id": req.change_id,
                    "school_id": req.school_id,
                    "docent_id": req.docent_id,
                    "venue_id": req.venue_id,
                    "activity_date": req.activity_date,
                    "note": req.note,
                    "approved_by": req.approved_by,
                    "approved_at": now,
                }
            }
            conn.execute(
                """INSERT INTO outbox_event
                       (event_id, event_type, tenant_id, aggregate_id, payload_json,
                        status, attempt_count, max_attempts, next_attempt_at,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?)""",
                (
                    event_id, EVENT_SCHEDULE_APPROVED, req.school_id, req.change_id,
                    json.dumps(payload, ensure_ascii=False),
                    self._max_attempts, now, now, now,
                ),
            )

            # 3) 同事务为每个接收方写入投递记录（含稳定幂等键与各自内容）
            for role, recipient_id, content in self._renderer(req):
                conn.execute(
                    """INSERT INTO delivery
                           (delivery_id, event_id, recipient_role, recipient_id,
                            idempotency_key, content_json, status, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)""",
                    (
                        f"dlv_{uuid.uuid4().hex}", event_id, role, recipient_id,
                        f"{event_id}:{role}", json.dumps(content, ensure_ascii=False), now,
                    ),
                )
            return ApprovalResult(req.change_id, event_id, STATE_APPROVED, False)

    def list_tenant_notifications(self, tenant_id: str) -> list[dict]:
        """按学校租户列出通知（含各接收方结果），不触碰其他租户数据。"""
        with self._db.read() as conn:
            events = conn.execute(
                "SELECT * FROM outbox_event WHERE tenant_id = ?"
                " ORDER BY created_at DESC, event_id DESC",
                (tenant_id,),
            ).fetchall()
            result = []
            for ev in events:
                deliveries = conn.execute(
                    "SELECT * FROM delivery WHERE event_id = ? ORDER BY recipient_role",
                    (ev["event_id"],),
                ).fetchall()
                result.append(
                    {
                        "event_id": ev["event_id"],
                        "event_type": ev["event_type"],
                        "status": ev["status"],
                        "created_at": ev["created_at"],
                        "change": json.loads(ev["payload_json"])["change"],
                        "deliveries": [
                            {
                                "recipient_role": d["recipient_role"],
                                "recipient_id": d["recipient_id"],
                                "status": d["status"],
                                "delivered_at": d["delivered_at"],
                                "content": json.loads(d["content_json"]),
                            }
                            for d in deliveries
                        ],
                    }
                )
            return result

    def backlog(self) -> dict:
        """积压视图：供管理命令与运营端点使用，只含计数不含通知内容。"""
        now_dt = self._clock.now()
        now = to_iso(now_dt)
        with self._db.read() as conn:
            events = {s: 0 for s in ("pending", "processing", "done", "dead")}
            for row in conn.execute(
                "SELECT status, COUNT(*) AS c FROM outbox_event GROUP BY status"
            ).fetchall():
                events[row["status"]] = row["c"]
            due_now = conn.execute(
                "SELECT COUNT(*) AS c FROM outbox_event"
                " WHERE status = 'pending' AND next_attempt_at <= ?",
                (now,),
            ).fetchone()["c"]
            oldest = conn.execute(
                "SELECT MIN(created_at) AS oldest FROM outbox_event"
                " WHERE status IN ('pending', 'processing')"
            ).fetchone()["oldest"]
            deliveries = {s: 0 for s in ("pending", "delivered", "dead")}
            for row in conn.execute(
                "SELECT status, COUNT(*) AS c FROM delivery GROUP BY status"
            ).fetchall():
                deliveries[row["status"]] = row["c"]
            attempts = conn.execute("SELECT COUNT(*) AS c FROM delivery_attempt").fetchone()["c"]
            redrives = conn.execute("SELECT COUNT(*) AS c FROM redrive_audit").fetchone()["c"]
        age = None
        if oldest is not None:
            age = max(0.0, (now_dt - from_iso(oldest)).total_seconds())
        return {
            "generated_at": now,
            "events": events,
            "due_now": due_now,
            "oldest_backlog_age_seconds": age,
            "deliveries": deliveries,
            "attempts_total": attempts,
            "redrives_total": redrives,
        }

    def redrive_event(self, event_id: str, *, actor: str, reason: str) -> dict:
        """人工重投终止失败的事件，并写入重投审计（不变量「失败重投审计」）。"""
        if not reason.strip():
            raise ValueError("人工重投必须填写原因")
        now = to_iso(self._clock.now())
        with self._db.transaction() as conn:
            ev = conn.execute(
                "SELECT * FROM outbox_event WHERE event_id = ?", (event_id,)
            ).fetchone()
            if ev is None:
                raise NotFoundError(f"事件 {event_id} 不存在")
            if ev["status"] != "dead":
                raise InvalidStateError(
                    f"仅终止失败（dead）的事件可以人工重投，当前状态为 {ev['status']}"
                )
            conn.execute(
                """UPDATE outbox_event SET
                       status = 'pending', attempt_count = 0, next_attempt_at = ?,
                       claimed_by = NULL, claimed_until = NULL, updated_at = ?
                   WHERE event_id = ?""",
                (now, now, event_id),
            )
            conn.execute(
                """UPDATE delivery SET
                       status = 'pending', attempt_count = 0, last_error = NULL, updated_at = ?
                   WHERE event_id = ? AND status = 'dead'""",
                (now, event_id),
            )
            cur = conn.execute(
                """INSERT INTO redrive_audit (event_id, actor, reason, from_status, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (event_id, actor, reason, ev["status"], now),
            )
            return {"event_id": event_id, "audit_id": cur.lastrowid, "status": "pending"}
