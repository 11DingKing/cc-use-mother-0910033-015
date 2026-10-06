"""可注入的时钟，便于在测试中确定性地推进租约与退避时间。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    """生产环境使用的 UTC 时钟。"""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def to_text(value: datetime) -> str:
    """统一的 UTC ISO8601 文本格式，字典序与时间序一致。"""
    if value.tzinfo is None:
        raise ValueError("时间必须带时区")
    return value.astimezone(timezone.utc).isoformat()


def parse_text(value: str) -> datetime:
    return datetime.fromisoformat(value)
