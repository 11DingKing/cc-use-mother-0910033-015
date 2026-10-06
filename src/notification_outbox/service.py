"""排班变更业务用例：批准排班与写入通知事件在同一事务完成。"""
from __future__ import annotations

import sqlite3

from .clock import Clock, SystemClock, to_text
from .models import Channel, NotificationEvent, Recipient

SCHEDULE_SCHEMA = """
CREATE TABLE IF NOT EXISTS schedule_change (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    school_id      INTEGER NOT NULL,
    venue_id       INTEGER NOT NULL,
    docent_id      INTEGER NOT NULL,
    school_contact TEXT    NOT NULL,
    docent_contact TEXT    NOT NULL,
    venue_contact  TEXT    NOT NULL,
    school_name    TEXT    NOT NULL,
    venue_name     TEXT    NOT NULL,
    docent_name    TEXT    NOT NULL,
    visit_date     TEXT    NOT NULL,
    start_time     TEXT    NOT NULL,
    status         TEXT    NOT NULL DEFAULT '待确认',
    updated_at     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schedule_school ON schedule_change(school_id, id);
"""


def initialize_business(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEDULE_SCHEMA)


class BusinessError(Exception):
    """业务规则冲突，HTTP 层映射为 4xx。"""

    def __init__(self, message: str, code: int = 400) -> None:
        super().__init__(message)
        self.code = code


def create_change(conn: sqlite3.Connection, data: dict, *, clock: Clock = SystemClock()) -> int:
    required = (
        "school_id", "venue_id", "docent_id",
        "school_contact", "docent_contact", "venue_contact",
        "school_name", "venue_name", "docent_name",
        "visit_date", "start_time",
    )
    missing = [k for k in required if not data.get(k)]
    if missing:
        raise BusinessError("缺少字段：" + "、".join(missing))
    school_id = data["school_id"]
    if isinstance(school_id, bool) or not isinstance(school_id, int) or school_id <= 0:
        raise BusinessError("school_id 必须为正整数")
    now = to_text(clock.now())
    cur = conn.execute(
        """
        INSERT INTO schedule_change
            (school_id, venue_id, docent_id, school_contact, docent_contact,
             venue_contact, school_name, venue_name, docent_name,
             visit_date, start_time, status, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '待确认', ?)
        """,
        tuple(data[k] for k in required) + (now,),
    )
    return int(cur.lastrowid)


def approve_change(
    conn: sqlite3.Connection,
    change_id: int,
    *,
    school_id: int,
    clock: Clock = SystemClock(),
) -> int:
    """批准排班变更：业务状态更新与通知事件同事务写入。

    调用方必须在外层事务内调用；若通知写入失败，业务批准一并回滚。
    返回投递箱事件 ID。
    """
    row = conn.execute(
        "SELECT * FROM schedule_change WHERE id = ? AND school_id = ?",
        (change_id, school_id),
    ).fetchone()
    if row is None:
        raise BusinessError("排班变更不存在或不属于本校", code=404)
    if row["status"] == "已排定":
        # 已批准过：幂等返回既有事件，重试请求不产生重复通知。
        existing = conn.execute(
            "SELECT id FROM outbox_event WHERE idempotency_key = ?",
            (_approval_key(change_id),),
        ).fetchone()
        if existing is not None:
            return int(existing["id"])
        raise BusinessError("排班已批准但投递箱事件缺失，请联系管理员")
    if row["status"] != "待确认":
        raise BusinessError(f"当前状态 {row['status']} 不可批准")

    now = to_text(clock.now())
    conn.execute(
        "UPDATE schedule_change SET status = '已排定', updated_at = ? WHERE id = ?",
        (now, change_id),
    )

    common = {
        "change_id": f"SC-{change_id:06d}",
        "school_name": row["school_name"],
        "venue_name": row["venue_name"],
        "docent_name": row["docent_name"],
        "visit_date": row["visit_date"],
        "start_time": row["start_time"],
    }
    event = NotificationEvent(
        event_type="schedule_change.approved",
        aggregate_type="schedule_change",
        aggregate_id=str(change_id),
        school_id=school_id,
        payload={**common, "status": "已排定"},
        # 业务变更单号即幂等键：请求超时重试不会重复发通知。
        idempotency_key=_approval_key(change_id),
        recipients=(
            Recipient(Channel.SCHOOL, row["school_contact"],
                      "schedule_approved_school", common),
            Recipient(Channel.DOCENT, row["docent_contact"],
                      "schedule_approved_docent", common),
            Recipient(Channel.VENUE, row["venue_contact"],
                      "schedule_approved_venue", common),
        ),
    )
    from .repositories import add_event

    return add_event(conn, event, clock=clock)


def _approval_key(change_id: int) -> str:
    return f"schedule-change-approved:{change_id}"
