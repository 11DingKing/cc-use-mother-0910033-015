"""变更通知投递箱：事务写入、租约认领、幂等投递与失败重投审计。"""
from __future__ import annotations

from .clock import Clock, SystemClock
from .database import connect, initialize, transaction
from .models import Channel, NotificationEvent, Recipient, RecipientStatus
from .repositories import add_event, backlog_summary
from .senders import (
    OutboundMessage,
    PermanentError,
    Sender,
    TransientError,
)
from .worker import DeliveryWorker, reinject_dead

__all__ = [
    "Channel",
    "Clock",
    "DeliveryWorker",
    "NotificationEvent",
    "OutboundMessage",
    "PermanentError",
    "Recipient",
    "RecipientStatus",
    "Sender",
    "SystemClock",
    "add_event",
    "backlog_summary",
    "connect",
    "initialize",
    "reinject_dead",
    "transaction",
    "TransientError",
]
