"""SQLite 存储层：模式定义与事务助手。

所有表都在单库内，业务表与投递箱表共享同一事务，保证「业务变更与通知事件同生共死」。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS schedule_change (
    change_id     TEXT PRIMARY KEY,
    school_id     TEXT NOT NULL,
    docent_id     TEXT NOT NULL,
    venue_id      TEXT NOT NULL,
    activity_date TEXT NOT NULL,
    state         TEXT NOT NULL,
    approved_by   TEXT NOT NULL,
    approved_at   TEXT NOT NULL,
    note          TEXT NOT NULL DEFAULT '',
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_event (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'done', 'dead')),
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 8,
    next_attempt_at TEXT NOT NULL,
    claimed_by      TEXT,
    claimed_until   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (event_type, aggregate_id)
);
CREATE INDEX IF NOT EXISTS ix_outbox_claim ON outbox_event (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS ix_outbox_tenant ON outbox_event (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS delivery (
    delivery_id     TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES outbox_event (event_id),
    recipient_role  TEXT NOT NULL,
    recipient_id    TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    content_json    TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'dead')),
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    delivered_at    TEXT,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_delivery_event ON delivery (event_id, status);

CREATE TABLE IF NOT EXISTS delivery_attempt (
    attempt_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id  TEXT NOT NULL REFERENCES delivery (delivery_id),
    event_id     TEXT NOT NULL,
    worker_id    TEXT NOT NULL,
    outcome      TEXT NOT NULL CHECK (outcome IN ('delivered', 'duplicate', 'failed')),
    error        TEXT,
    attempted_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_attempt_delivery ON delivery_attempt (delivery_id);

CREATE TABLE IF NOT EXISTS redrive_audit (
    audit_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL REFERENCES outbox_event (event_id),
    actor       TEXT NOT NULL,
    reason      TEXT NOT NULL,
    from_status TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
"""


class Database:
    """SQLite 数据库句柄工厂；每次操作使用独立连接，便于多线程与多进程共用。"""

    def __init__(self, path: str | Path):
        self.path = str(path)
        self.init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def init_schema(self) -> None:
        conn = self.connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """写事务：BEGIN IMMEDIATE 先取写锁，避免升级死锁；异常即整体回滚。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()
