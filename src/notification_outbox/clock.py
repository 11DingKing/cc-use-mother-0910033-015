"""时钟抽象：便于测试注入确定时间。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime) -> str:
    """统一序列化为可字典序比较的 UTC ISO 字符串。"""
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def from_iso(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(timezone.utc)


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return utcnow()


class ManualClock:
    """测试用手动时钟，可确定性地推进时间。"""

    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2026, 10, 5, 8, 0, 0, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)
