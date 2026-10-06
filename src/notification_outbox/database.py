"""数据库连接与表结构（SQLite 实现）。

表设计对应领域契约的四个不变量：
- ``outbox_event`` / ``outbox_recipient``：业务变更与通知事件同事务写入（事务投递箱）。
- ``lease_owner`` / ``leased_until`` / ``claim_batch``：租约批量认领。
- ``idempotency_key``：接收方幂等键。
- 投递结果行与 ``dead`` 终态：失败重投审计。
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

SCHEMA_VERSION = 1

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS outbox_event (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT    NOT NULL,
    aggregate_type  TEXT    NOT NULL,
    aggregate_id    TEXT    NOT NULL,
    school_id       INTEGER NOT NULL,
    payload         TEXT    NOT NULL,
    idempotency_key TEXT    NOT NULL,
    created_at      TEXT    NOT NULL,
    UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_event_school ON outbox_event(school_id, id);

CREATE TABLE IF NOT EXISTS outbox_recipient (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     INTEGER NOT NULL REFERENCES outbox_event(id),
    school_id    INTEGER NOT NULL,
    channel      TEXT    NOT NULL CHECK (channel IN ('school','docent','venue')),
    address      TEXT    NOT NULL,
    template     TEXT    NOT NULL,
    params       TEXT    NOT NULL DEFAULT '{}',
    status       TEXT    NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','leased','succeeded','dead','reopened')),
    attempts     INTEGER NOT NULL DEFAULT 0,
    not_before   TEXT    NOT NULL,
    lease_owner  TEXT,
    leased_until TEXT,
    last_error   TEXT,
    updated_at   TEXT    NOT NULL,
    succeeded_at TEXT,
    dead_at      TEXT,
    -- 同一事件、同一接收方渠道、同一地址只允许一条投递记录，
    -- 从存储层杜绝同一接收方收到重复通知。
    UNIQUE (event_id, channel, address)
);

-- 工作进程的认领主索引：可投递且到期的行集中在最前。
CREATE INDEX IF NOT EXISTS idx_recipient_dispatch
    ON outbox_recipient(status, not_before);
CREATE INDEX IF NOT EXISTS idx_recipient_event ON outbox_recipient(event_id);
CREATE INDEX IF NOT EXISTS idx_recipient_school ON outbox_recipient(school_id, id);

CREATE TABLE IF NOT EXISTS delivery_attempt (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_id INTEGER NOT NULL REFERENCES outbox_recipient(id),
    attempt_no   INTEGER NOT NULL,
    lease_owner  TEXT    NOT NULL,
    result       TEXT    NOT NULL CHECK (result IN ('success','failure','dead')),
    error_code   TEXT,
    error_detail TEXT,
    created_at   TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attempt_recipient ON delivery_attempt(recipient_id, id);

-- 人工操作审计（重投等），对应"失败重投审计"不变量。
CREATE TABLE IF NOT EXISTS outbox_admin_action (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_id INTEGER NOT NULL REFERENCES outbox_recipient(id),
    action       TEXT    NOT NULL CHECK (action IN ('reinject')),
    actor        TEXT    NOT NULL,
    note         TEXT    NOT NULL DEFAULT '',
    created_at   TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_admin_action_recipient
    ON outbox_admin_action(recipient_id, id);
"""


def connect(path: str | Path = ":memory:") -> sqlite3.Connection:
    """打开一个为并发投递箱场景配置好的连接。"""
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # busy_timeout 配合 WAL，允许多个工作进程并发认领。
    conn.execute(f"PRAGMA busy_timeout={30_000}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialize(conn: sqlite3.Connection) -> None:
    """幂等地创建全部表与索引。"""
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """显式事务上下文。连接使用 autocommit（isolation_level=None），
    事务边界完全由本层控制，便于与业务库同事务写入。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
