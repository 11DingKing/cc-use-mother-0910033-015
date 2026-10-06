"""工作进程：租约批量认领、投递、退避重试、终止失败与审计。

可靠性语义（至少一次 + 接收方幂等）：

- 认领即获得带超时的租约（``leased_until``），进程崩溃或服务重启后，
  过期租约会被下一轮认领自动回收，消息不丢。
- 每次投递的成败都在独立事务中落盘，并写 ``delivery_attempt`` 审计行。
- 瞬时错误按指数退避重试；超过 ``max_attempts`` 进入 ``dead`` 终态，
  等待人工重投（见 :func:`reinject_dead`）。
- 同一条投递在租约过期后可能被发送两次，故每条消息携带稳定的
  ``dedupe_key``（事件幂等键 + 渠道 + 地址），由发送通道去重。
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
import uuid
from datetime import timedelta

from .clock import Clock, SystemClock, to_text
from .models import RecipientStatus
from .rendering import TemplateRenderer
from .senders import (
    OutboundMessage,
    PermanentError,
    Sender,
    TransientError,
)


class DeliveryWorker:
    def __init__(
        self,
        conn: sqlite3.Connection,
        sender: Sender,
        *,
        renderer: TemplateRenderer | None = None,
        worker_id: str | None = None,
        batch_size: int = 10,
        lease_seconds: float = 30.0,
        max_attempts: int = 8,
        base_delay_seconds: float = 5.0,
        max_delay_seconds: float = 3600.0,
        clock: Clock = SystemClock(),
        rng: random.Random | None = None,
    ) -> None:
        self.conn = conn
        self.sender = sender
        self.renderer = renderer or TemplateRenderer()
        self.worker_id = worker_id or f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.base_delay = base_delay_seconds
        self.max_delay = max_delay_seconds
        self.clock = clock
        self.rng = rng or random.Random()

    # ------------------------------------------------------------------ 认领

    def claim_batch(self) -> list[sqlite3.Row]:
        """原子地批量认领一批到期投递项。

        包含两类：
        1. ``pending``/``reopened`` 且 ``not_before`` 已到；
        2. ``leased`` 但租约已过期（前一个进程崩溃或重启）。
        """
        now = self.clock.now()
        now_text = to_text(now)
        lease_end = to_text(now + timedelta(seconds=self.lease_seconds))
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            ids = [
                row["id"]
                for row in self.conn.execute(
                    """
                    SELECT id FROM outbox_recipient
                    WHERE (status IN ('pending','reopened') AND not_before <= ?)
                       OR (status = 'leased' AND leased_until < ?)
                    ORDER BY not_before, id
                    LIMIT ?
                    """,
                    (now_text, now_text, self.batch_size),
                ).fetchall()
            ]
            if ids:
                self.conn.executemany(
                    """
                    UPDATE outbox_recipient
                    SET status = 'leased',
                        lease_owner = ?,
                        leased_until = ?,
                        attempts = attempts + 1,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    [
                        (self.worker_id, lease_end, now_text, rid)
                        for rid in ids
                    ],
                )
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

        if not ids:
            return []
        placeholders = ",".join("?" * len(ids))
        return list(
            self.conn.execute(
                f"""
                SELECT r.*, e.event_type, e.aggregate_type, e.aggregate_id,
                       e.payload AS event_payload, e.idempotency_key
                FROM outbox_recipient r
                JOIN outbox_event e ON e.id = r.event_id
                WHERE r.id IN ({placeholders})
                ORDER BY r.id
                """,
                ids,
            ).fetchall()
        )

    # -------------------------------------------------------------- 单轮处理

    def run_once(self) -> int:
        """认领并处理一批，返回处理条数。长驻循环在外部驱动。"""
        rows = self.claim_batch()
        for row in rows:
            self._deliver(row)
        return len(rows)

    def _deliver(self, row: sqlite3.Row) -> None:
        rid = row["id"]
        try:
            params = json.loads(row["params"])
            subject, body = self.renderer.render(row["template"], params)
            message = OutboundMessage(
                recipient_id=rid,
                channel=row["channel"],
                address=row["address"],
                subject=subject,
                body=body,
                # 稳定幂等键：租约过期导致的重复发送会被通道按此键去重。
                dedupe_key=f"{row['idempotency_key']}:{row['channel']}:{row['address']}",
                context={
                    "event_type": row["event_type"],
                    "aggregate_type": row["aggregate_type"],
                    "aggregate_id": row["aggregate_id"],
                    "event_payload": json.loads(row["event_payload"]),
                    "school_id": row["school_id"],
                    "attempt": row["attempts"],
                },
            )
            self.sender.send(message)
        except PermanentError as exc:
            self._mark_dead(rid, exc.code, exc.detail)
        except TransientError as exc:
            self._record_failure(rid, exc.code, exc.detail)
        except Exception as exc:  # 未分类异常一律按可恢复处理，避免误杀
            self._record_failure(rid, "unexpected_error", repr(exc))
        else:
            self._mark_succeeded(rid)

    # ------------------------------------------------------------ 结果落盘

    def _mark_succeeded(self, rid: int) -> None:
        now_text = to_text(self.clock.now())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self.conn.execute(
                """
                UPDATE outbox_recipient
                SET status = 'succeeded', lease_owner = NULL, leased_until = NULL,
                    last_error = NULL, succeeded_at = ?, updated_at = ?
                WHERE id = ? AND lease_owner = ?
                """,
                (now_text, now_text, rid, self.worker_id),
            )
            if cur.rowcount:
                self.conn.execute(
                    """
                    INSERT INTO delivery_attempt
                        (recipient_id, attempt_no, lease_owner, result, created_at)
                    SELECT id, attempts, ?, 'success', ?
                    FROM outbox_recipient WHERE id = ?
                    """,
                    (self.worker_id, now_text, rid),
                )
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def _record_failure(self, rid: int, code: str, detail: str) -> None:
        now = self.clock.now()
        now_text = to_text(now)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT attempts FROM outbox_recipient WHERE id = ? AND lease_owner = ?",
                (rid, self.worker_id),
            ).fetchone()
            if row is None:
                # 租约已被回收，交由新属主记账，避免覆盖他人结果。
                self.conn.execute("ROLLBACK")
                return
            attempts = row["attempts"]
            if attempts >= self.max_attempts:
                self.conn.execute(
                    """
                    UPDATE outbox_recipient
                    SET status = 'dead', lease_owner = NULL, leased_until = NULL,
                        last_error = ?, dead_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (f"[{code}] {detail}", now_text, now_text, rid),
                )
                self._insert_attempt(rid, attempts, "dead", code, detail, now_text)
            else:
                delay = self._backoff_delay(attempts)
                self.conn.execute(
                    """
                    UPDATE outbox_recipient
                    SET status = 'pending', lease_owner = NULL, leased_until = NULL,
                        not_before = ?, last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (to_text(now + timedelta(seconds=delay)),
                     f"[{code}] {detail}", now_text, rid),
                )
                self._insert_attempt(rid, attempts, "failure", code, detail, now_text)
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def _mark_dead(self, rid: int, code: str, detail: str) -> None:
        now_text = to_text(self.clock.now())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self.conn.execute(
                """
                UPDATE outbox_recipient
                SET status = 'dead', lease_owner = NULL, leased_until = NULL,
                    last_error = ?, dead_at = ?, updated_at = ?
                WHERE id = ? AND lease_owner = ?
                """,
                (f"[{code}] {detail}", now_text, now_text, rid, self.worker_id),
            )
            if cur.rowcount:
                row = self.conn.execute(
                    "SELECT attempts FROM outbox_recipient WHERE id = ?", (rid,)
                ).fetchone()
                self._insert_attempt(
                    rid, row["attempts"], "dead", code, detail, now_text
                )
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def _insert_attempt(
        self, rid: int, attempt_no: int, result: str,
        code: str, detail: str, now_text: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO delivery_attempt
                (recipient_id, attempt_no, lease_owner, result,
                 error_code, error_detail, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (rid, attempt_no, self.worker_id, result, code, detail, now_text),
        )

    def _backoff_delay(self, attempts: int) -> float:
        delay = min(
            self.max_delay,
            self.base_delay * (2 ** (attempts - 1)),
        )
        # ±50% 抖动，避免失败消息在同一刻集体重试；rng 可注入以便测试。
        return delay * (0.5 + self.rng.random())


def reinject_dead(
    conn: sqlite3.Connection,
    recipient_ids: list[int],
    *,
    actor: str,
    note: str = "",
    clock: Clock = SystemClock(),
) -> list[int]:
    """人工重投：把 dead 终态的投递项重新放回队列。

    审计动作写入 ``outbox_admin_action``；历史投递尝试仍保留在
    ``delivery_attempt`` 中，``attempts`` 清零开始新的投递周期，
    消息使用相同的 dedupe_key，通道侧不会重复入账。
    返回实际被重投的 ID 列表。
    """
    if not recipient_ids:
        return []
    now_text = to_text(clock.now())
    placeholders = ",".join("?" * len(recipient_ids))
    conn.execute("BEGIN IMMEDIATE")
    try:
        ids = [
            row["id"]
            for row in conn.execute(
                f"""
                SELECT id FROM outbox_recipient
                WHERE id IN ({placeholders}) AND status = 'dead'
                ORDER BY id
                """,
                recipient_ids,
            ).fetchall()
        ]
        if ids:
            conn.executemany(
                """
                UPDATE outbox_recipient
                SET status = 'reopened', attempts = 0, not_before = ?,
                        lease_owner = NULL, leased_until = NULL,
                        last_error = NULL, dead_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                [(now_text, now_text, rid) for rid in ids],
            )
            conn.executemany(
                """
                INSERT INTO outbox_admin_action
                    (recipient_id, action, actor, note, created_at)
                VALUES (?, 'reinject', ?, ?, ?)
                """,
                [(rid, actor, note, now_text) for rid in ids],
            )
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
    return ids
