"""投递箱仓储：事务写入事件与接收方，以及查询接口。

写入方法不自行提交，调用方应在与业务变更相同的事务中调用，例如::

    with transaction(conn):
        conn.execute("UPDATE schedule SET status = ? ...", (...))
        outbox.add_event(conn, event)

这样业务更新与通知事件原子提交，不会出现"更新成功但通知丢失"。
"""
from __future__ import annotations

import json
import sqlite3

from .clock import Clock, SystemClock, to_text
from .models import NotificationEvent, RecipientStatus


def add_event(
    conn: sqlite3.Connection,
    event: NotificationEvent,
    *,
    clock: Clock = SystemClock(),
) -> int:
    """在当前事务内写入通知事件及其全部接收方。

    返回事件 ID。重复的 ``idempotency_key`` 视为同一业务变更的重试，
    直接返回已有事件 ID 而不产生重复数据（接收方幂等键）。
    """
    now = to_text(clock.now())
    existing = conn.execute(
        "SELECT id FROM outbox_event WHERE idempotency_key = ?",
        (event.idempotency_key,),
    ).fetchone()
    if existing is not None:
        return int(existing["id"])

    cur = conn.execute(
        """
        INSERT INTO outbox_event
            (event_type, aggregate_type, aggregate_id, school_id,
             payload, idempotency_key, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_type,
            event.aggregate_type,
            event.aggregate_id,
            event.school_id,
            json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
            event.idempotency_key,
            now,
        ),
    )
    event_id = int(cur.lastrowid)
    conn.executemany(
        """
        INSERT INTO outbox_recipient
            (event_id, school_id, channel, address, template, params,
             status, not_before, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                event_id,
                event.school_id,
                r.channel.value,
                r.address,
                r.template,
                json.dumps(r.params, ensure_ascii=False, sort_keys=True),
                RecipientStatus.PENDING.value,
                now,
                now,
            )
            for r in event.recipients
        ],
    )
    return event_id


def backlog_summary(conn: sqlite3.Connection) -> dict:
    """全量积压统计（管理命令用，跨学校）。"""
    now = to_text(SystemClock().now())
    rows = conn.execute(
        """
        SELECT status, COUNT(*) AS n,
               SUM(CASE WHEN status IN ('pending','reopened')
                          AND not_before <= ? THEN 1
                         WHEN status = 'leased' AND leased_until < ? THEN 1
                         ELSE 0 END) AS due
        FROM outbox_recipient
        GROUP BY status
        """,
        (now, now),
    ).fetchall()
    result = {s.value: 0 for s in RecipientStatus}
    due = 0
    total = 0
    for row in rows:
        result[row["status"]] = row["n"]
        total += row["n"]
        due += row["due"] or 0
    result["total"] = total
    result["due_now"] = due
    return result


def list_recipients(
    conn: sqlite3.Connection,
    *,
    status: RecipientStatus | None = None,
    school_id: int | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[sqlite3.Row]:
    """列出投递项。``school_id`` 用于多租户隔离，普通 API 必须传入。"""
    clauses: list[str] = []
    params: list = []
    if status is not None:
        clauses.append("r.status = ?")
        params.append(status.value)
    if school_id is not None:
        clauses.append("r.school_id = ?")
        params.append(school_id)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.extend([limit, offset])
    return list(
        conn.execute(
            f"""
            SELECT r.*, e.event_type, e.aggregate_type, e.aggregate_id,
                   e.idempotency_key
            FROM outbox_recipient r
            JOIN outbox_event e ON e.id = r.event_id
            {where}
            ORDER BY r.id
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
    )


def get_event(
    conn: sqlite3.Connection, event_id: int, *, school_id: int | None = None
) -> sqlite3.Row | None:
    """读取事件。传入 ``school_id`` 时强制租户隔离，查不到（含跨校）返回 None。"""
    sql = "SELECT * FROM outbox_event WHERE id = ?"
    params: list = [event_id]
    if school_id is not None:
        sql += " AND school_id = ?"
        params.append(school_id)
    return conn.execute(sql, params).fetchone()


def list_event_recipients(
    conn: sqlite3.Connection, event_id: int, *, school_id: int | None = None
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM outbox_recipient WHERE event_id = ?"
    params: list = [event_id]
    if school_id is not None:
        sql += " AND school_id = ?"
        params.append(school_id)
    sql += " ORDER BY id"
    return list(conn.execute(sql, params).fetchall())


def list_attempts(conn: sqlite3.Connection, recipient_id: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT * FROM delivery_attempt WHERE recipient_id = ? ORDER BY id",
            (recipient_id,),
        ).fetchall()
    )


def recipient_by_id(
    conn: sqlite3.Connection, recipient_id: int, *, school_id: int | None = None
) -> sqlite3.Row | None:
    sql = "SELECT * FROM outbox_recipient WHERE id = ?"
    params: list = [recipient_id]
    if school_id is not None:
        sql += " AND school_id = ?"
        params.append(school_id)
    return conn.execute(sql, params).fetchone()
