"""通知发送通道抽象与测试/演示实现。"""
from __future__ import annotations

import sys
from typing import Protocol


class Sender(Protocol):
    """发送通道协议。

    返回值："delivered" 表示首次送达，"duplicate" 表示幂等键命中（通道侧去重，
    未产生重复消息）。失败时抛出 TransientSendError / PermanentSendError。
    """

    def send(
        self,
        *,
        idempotency_key: str,
        recipient_role: str,
        recipient_id: str,
        content: dict,
    ) -> str: ...


class RecordingSender:
    """按接收方幂等键去重的记录型通道（测试与演示用）。"""

    def __init__(self) -> None:
        self.messages: dict[str, dict] = {}
        self.call_count = 0

    def send(self, *, idempotency_key, recipient_role, recipient_id, content) -> str:
        self.call_count += 1
        if idempotency_key in self.messages:
            return "duplicate"
        self.messages[idempotency_key] = {
            "recipient_role": recipient_role,
            "recipient_id": recipient_id,
            "content": content,
        }
        return "delivered"


class LoggingSender:
    """命令行演示用：日志写 stderr（stdout 留给报告），进程内按幂等键去重。"""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def send(self, *, idempotency_key, recipient_role, recipient_id, content) -> str:
        if idempotency_key in self._seen:
            print(f"[outbox] 幂等键 {idempotency_key} 已送达，跳过重复发送", file=sys.stderr)
            return "duplicate"
        self._seen.add(idempotency_key)
        print(
            f"[outbox] -> {recipient_role}/{recipient_id}: {content['title']} | {content['body']}",
            file=sys.stderr,
        )
        return "delivered"
