"""测试公共工具：内存库、假时钟、可编程的假发送器。"""
from __future__ import annotations

import random
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


@contextmanager
def shared_scope(conn):
    """测试用：复用同一个连接，出作用域不关闭。"""
    yield conn

from notification_outbox.database import connect, initialize  # noqa: E402
from notification_outbox.service import initialize_business  # noqa: E402
from notification_outbox.senders import (  # noqa: E402
    OutboundMessage,
    PermanentError,
    TransientError,
)


CHANGE_DATA = {
    "school_id": 1,
    "venue_id": 10,
    "docent_id": 100,
    "school_contact": "school@a.edu",
    "docent_contact": "docent@guide.cn",
    "venue_contact": "venue@museum.cn",
    "school_name": "第一中学",
    "venue_name": "城市博物馆",
    "docent_name": "李讲解",
    "visit_date": "2026-10-20",
    "start_time": "09:30",
}


def make_db() -> sqlite3.Connection:
    conn = connect(":memory:")
    initialize(conn)
    initialize_business(conn)
    return conn


class FakeClock:
    def __init__(self, start: datetime | None = None) -> None:
        self.now_value = start or datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.now_value

    def advance(self, seconds: float) -> None:
        self.now_value += timedelta(seconds=seconds)


class FixedRandom(random.Random):
    """让退避抖动可预测：random() 恒为 0.5，抖动因子 (0.5+r)=1.0，退避取基准值。"""

    def random(self) -> float:  # type: ignore[override]
        return 0.5


class ScriptedSender:
    """按 dedupe_key 的第 n 次发送脚本化返回结果。

    脚本元素：None 表示成功，或 ``("transient", code)`` / ``("permanent", code)``。
    超出脚本长度后默认成功。
    """

    def __init__(self, script: dict[str, list] | None = None) -> None:
        self.script = script or {}
        self.calls: list[OutboundMessage] = []
        self._counts: dict[str, int] = {}

    def send(self, message: OutboundMessage) -> None:
        self.calls.append(message)
        n = self._counts.get(message.dedupe_key, 0)
        self._counts[message.dedupe_key] = n + 1
        outcomes = self.script.get(message.dedupe_key, [])
        outcome = outcomes[n] if n < len(outcomes) else None
        if outcome is None:
            return
        kind, code = outcome
        if kind == "transient":
            raise TransientError(code, f"模拟瞬时失败 #{n + 1}")
        if kind == "permanent":
            raise PermanentError(code, "模拟永久失败")
        raise AssertionError(f"未知脚本动作：{outcome}")

    def keys_delivered(self) -> list[str]:
        return [m.dedupe_key for m in self.calls]
