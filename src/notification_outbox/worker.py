"""投递工作进程：租约批量认领、按接收方去重投递、退避重试与终止失败。

- 租约批量认领：claim_batch 在单事务内把一批可投递事件置为 processing 并记录
  claimed_by / claimed_until；租约到期未被完成的事件可被任意工作进程重新认领，
  因此工作进程崩溃或服务重启都不会丢消息。
- 去重：每个接收方投递携带稳定幂等键，通道侧按幂等键去重；「已送达但结果未记录」
  的尝试在重试时记为 duplicate，不再产生第二条消息。
- 每个接收方结果：delivery_attempt 追加记录每次尝试，delivery 记录最终状态。
"""
from __future__ import annotations

import json
import socket
import uuid
from datetime import timedelta

from .clock import Clock, SystemClock, to_iso
from .db import Database
from .errors import PermanentSendError
from .sender import Sender


class OutboxWorker:
    def __init__(
        self,
        db: Database,
        sender: Sender,
        *,
        clock: Clock = SystemClock(),
        worker_id: str | None = None,
        batch_size: int = 10,
        lease_seconds: float = 30.0,
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 300.0,
    ):
        if batch_size < 1:
            raise ValueError("batch_size 必须 >= 1")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds 必须 > 0")
        if base_backoff_seconds <= 0:
            raise ValueError("base_backoff_seconds 必须 > 0")
        self._db = db
        self._sender = sender
        self._clock = clock
        self.worker_id = worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds

    def backoff_seconds(self, attempt: int) -> float:
        """指数退避：base * 2^(attempt-1)，封顶 max_backoff_seconds。"""
        return min(self.max_backoff_seconds, self.base_backoff_seconds * (2 ** (attempt - 1)))

    def claim_batch(self) -> list[str]:
        """租约批量认领：返回本进程租下的事件 id 列表。"""
        now_dt = self._clock.now()
        now = to_iso(now_dt)
        until = to_iso(now_dt + timedelta(seconds=self.lease_seconds))
        with self._db.transaction() as conn:
            rows = conn.execute(
                """SELECT event_id FROM outbox_event
                   WHERE (status = 'pending' AND next_attempt_at <= ?)
                      OR (status = 'processing' AND claimed_until IS NOT NULL
                          AND claimed_until <= ?)
                   ORDER BY next_attempt_at
                   LIMIT ?""",
                (now, now, self.batch_size),
            ).fetchall()
            event_ids = [row["event_id"] for row in rows]
            for event_id in event_ids:
                conn.execute(
                    """UPDATE outbox_event SET
                           status = 'processing', claimed_by = ?, claimed_until = ?, updated_at = ?
                       WHERE event_id = ?""",
                    (self.worker_id, until, now, event_id),
                )
            return event_ids

    def run_once(self) -> dict:
        """认领一批并逐个处理，返回本轮统计。"""
        report = {"claimed": 0, "done": 0, "retry": 0, "dead": 0, "skipped": 0}
        event_ids = self.claim_batch()
        report["claimed"] = len(event_ids)
        for event_id in event_ids:
            report[self.process_event(event_id)] += 1
        return report

    def run_until_idle(self) -> dict:
        """持续认领处理，直到当前没有可认领的事件（退避中的事件不算）。"""
        total = {"claimed": 0, "done": 0, "retry": 0, "dead": 0, "skipped": 0}
        while True:
            report = self.run_once()
            for key, value in report.items():
                total[key] += value
            if report["claimed"] == 0:
                return total

    def process_event(self, event_id: str) -> str:
        """处理单个已认领事件，返回 done / retry / dead / skipped。"""
        now_dt = self._clock.now()
        now = to_iso(now_dt)
        with self._db.transaction() as conn:
            ev = conn.execute(
                "SELECT * FROM outbox_event WHERE event_id = ?", (event_id,)
            ).fetchone()
            if ev is None:
                return "skipped"
            if ev["status"] != "processing" or ev["claimed_by"] != self.worker_id:
                return "skipped"  # 租约已被其他工作进程接管

            deliveries = conn.execute(
                "SELECT * FROM delivery WHERE event_id = ? AND status = 'pending'"
                " ORDER BY recipient_role",
                (event_id,),
            ).fetchall()
            for dlv in deliveries:
                self._attempt_delivery(conn, ev, dlv, now)

            counts = {
                row["status"]: row["c"]
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS c FROM delivery WHERE event_id = ? GROUP BY status",
                    (event_id,),
                ).fetchall()
            }
            pending = counts.get("pending", 0)
            dead = counts.get("dead", 0)

            if pending == 0 and dead == 0:
                self._finish_event(conn, event_id, "done", now)
                return "done"
            if pending == 0:
                # 只剩终止失败的接收方，等待人工重投
                self._finish_event(conn, event_id, "dead", now)
                return "dead"

            attempts = ev["attempt_count"] + 1
            if attempts >= ev["max_attempts"]:
                conn.execute(
                    """UPDATE delivery SET status = 'dead', last_error = ?, updated_at = ?
                       WHERE event_id = ? AND status = 'pending'""",
                    ("超过最大重试次数，转人工处理", now, event_id),
                )
                self._finish_event(conn, event_id, "dead", now)
                return "dead"

            next_at = to_iso(now_dt + timedelta(seconds=self.backoff_seconds(attempts)))
            conn.execute(
                """UPDATE outbox_event SET
                       status = 'pending', attempt_count = ?, next_attempt_at = ?,
                       claimed_by = NULL, claimed_until = NULL, updated_at = ?
                   WHERE event_id = ?""",
                (attempts, next_at, now, event_id),
            )
            return "retry"

    def _attempt_delivery(self, conn, ev, dlv, now: str) -> None:
        """投递单个接收方并记录结果；通道异常不中断同批其他接收方。"""
        permanent = False
        try:
            outcome = self._sender.send(
                idempotency_key=dlv["idempotency_key"],
                recipient_role=dlv["recipient_role"],
                recipient_id=dlv["recipient_id"],
                content=json.loads(dlv["content_json"]),
            )
            error = None
        except PermanentSendError as exc:
            outcome, error, permanent = "failed", f"permanent: {exc}", True
        except Exception as exc:  # 超时、网络抖动等一律按可重试处理
            outcome, error = "failed", f"transient: {exc}"

        conn.execute(
            """INSERT INTO delivery_attempt
                   (delivery_id, event_id, worker_id, outcome, error, attempted_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (dlv["delivery_id"], ev["event_id"], self.worker_id, outcome, error, now),
        )
        if outcome in ("delivered", "duplicate"):
            # duplicate：此前某次尝试已送达但结果未记录，本次仅补记，不重复发送
            conn.execute(
                """UPDATE delivery SET
                       status = 'delivered', attempt_count = attempt_count + 1,
                       last_error = NULL, delivered_at = ?, updated_at = ?
                   WHERE delivery_id = ?""",
                (now, now, dlv["delivery_id"]),
            )
        elif permanent:
            conn.execute(
                """UPDATE delivery SET
                       status = 'dead', attempt_count = attempt_count + 1,
                       last_error = ?, updated_at = ?
                   WHERE delivery_id = ?""",
                (error, now, dlv["delivery_id"]),
            )
        else:
            conn.execute(
                """UPDATE delivery SET
                       attempt_count = attempt_count + 1, last_error = ?, updated_at = ?
                   WHERE delivery_id = ?""",
                (error, now, dlv["delivery_id"]),
            )

    @staticmethod
    def _finish_event(conn, event_id: str, status: str, now: str) -> None:
        conn.execute(
            """UPDATE outbox_event SET
                   status = ?, claimed_by = NULL, claimed_until = NULL, updated_at = ?
               WHERE event_id = ?""",
            (status, now, event_id),
        )
