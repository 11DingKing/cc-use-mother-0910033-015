"""投递箱领域错误。"""


class OutboxError(Exception):
    """领域错误基类。"""


class NotFoundError(OutboxError):
    """目标记录不存在。"""


class ConflictError(OutboxError):
    """请求与现有状态冲突（如重复批准但内容不一致）。"""


class InvalidStateError(OutboxError):
    """当前状态不允许该操作（如对未终止的事件重投）。"""


class TransientSendError(OutboxError):
    """可重试的发送失败（超时、限流等）。"""


class PermanentSendError(OutboxError):
    """不可重试的发送失败（接收方不存在等），直接终止该接收方。"""
