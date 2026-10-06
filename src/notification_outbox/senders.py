"""通知发送通道抽象。

真实部署中把短信、邮件、企业微信等适配器实现为 :class:`Sender`。
发送必须是幂等的或至少可安全重试：``dedupe_key`` 会随每次投递传入，
支持端到端去重的通道可据此拒绝重复消息。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class PermanentError(Exception):
    """不可恢复错误（如地址不存在）：不再退避重试，直接进入 dead。"""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"[{code}] {detail}")
        self.code = code
        self.detail = detail


class TransientError(Exception):
    """可恢复错误（超时、5xx、限流）：按退避策略重试。"""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"[{code}] {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class OutboundMessage:
    """工作进程交给发送器的一条已渲染消息。"""

    recipient_id: int
    channel: str
    address: str
    subject: str
    body: str
    dedupe_key: str
    context: dict[str, Any]


class Sender(Protocol):
    def send(self, message: OutboundMessage) -> None:
        """发送成功返回；失败抛 :class:`TransientError` 或 :class:`PermanentError`。

        约定：只有在发送方确认消息已被接受后才能正常返回；
        网络超时时一律抛 TransientError，宁可重复发送（由 dedupe_key 去重），
        也不能在不确定时把消息标记为成功。
        """
        ...
