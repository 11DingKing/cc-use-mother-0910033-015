"""领域数据模型：通知事件与接收方投递项。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class Channel(str, enum.Enum):
    """三类接收方：学校、讲解员、场馆。"""

    SCHOOL = "school"
    DOCENT = "docent"
    VENUE = "venue"


class RecipientStatus(str, enum.Enum):
    PENDING = "pending"      # 等待首次投递
    LEASED = "leased"        # 已被某工作进程按租约认领
    SUCCEEDED = "succeeded"  # 终态：投递成功
    DEAD = "dead"            # 终态：超过最大尝试次数，等待人工重投
    REOPENED = "reopened"    # 人工重投后重新进入队列


@dataclass(frozen=True)
class Recipient:
    """一个接收方的一条通知及其渲染参数。"""

    channel: Channel
    address: str
    template: str
    params: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.address or not self.address.strip():
            raise ValueError("接收方地址不能为空")
        if not self.template or not self.template.strip():
            raise ValueError("通知模板不能为空")
        if not isinstance(self.channel, Channel):
            raise ValueError("channel 必须是 Channel 枚举")


@dataclass(frozen=True)
class NotificationEvent:
    """一次业务变更所对应的通知事件，包含该事件的全部接收方。

    ``idempotency_key`` 由业务方按业务变更生成（如排班变更单号），
    重试同一请求不会产生第二条事件。
    """

    event_type: str
    aggregate_type: str
    aggregate_id: str
    school_id: int
    payload: dict
    idempotency_key: str
    recipients: tuple[Recipient, ...]

    def __post_init__(self) -> None:
        if not self.event_type:
            raise ValueError("event_type 不能为空")
        if not self.aggregate_id:
            raise ValueError("aggregate_id 不能为空")
        if isinstance(self.school_id, bool) or not isinstance(self.school_id, int):
            raise ValueError("school_id 必须是整数")
        if self.school_id <= 0:
            raise ValueError("school_id 必须为正整数，用于多租户隔离")
        if not self.idempotency_key or not self.idempotency_key.strip():
            raise ValueError("idempotency_key 不能为空")
        if not isinstance(self.payload, dict):
            raise ValueError("payload 必须是字典")
        if not self.recipients:
            raise ValueError("通知事件至少包含一个接收方")
        channels = {(r.channel, r.address) for r in self.recipients}
        if len(channels) != len(self.recipients):
            raise ValueError("同一事件内 (channel, address) 不能重复")
