"""可区分的错误类型：提交冲突、容量拒绝、失败分类。"""


class TaskError(Exception):
    """所有任务错误的基类，携带稳定的错误码。"""

    code: str = "task_error"


class DuplicateSubmissionError(TaskError):
    """相同幂等键、相同内容的重复提交。

    设计上重复提交不是错误：服务层返回 202 + created=false /
    reason=duplicate_replay 复用既有任务。保留该错误码仅用于语义文档化
    及未来“严格模式”下需要显式拒绝重放的场景。
    """

    code = "duplicate_submission"


class IdempotencyConflictError(TaskError):
    """相同幂等键携带不同请求内容：明确拒绝。"""

    code = "idempotency_content_conflict"


class QueueFullError(TaskError):
    """并发槽位与等待队列均已满：按可区分原因拒绝，不静默丢弃。"""

    code = "capacity_queue_full"


class TaskNotFoundError(TaskError):
    """任务标识不存在。"""

    code = "task_not_found"


class TransientError(TaskError):
    """瞬时失败：可安全重试（副作用由处理器自行保证幂等）。"""

    code = "transient_failure"


class PermanentError(TaskError):
    """永久失败：不重试，直接终结。"""

    code = "permanent_failure"


class TaskTimeoutError(TaskError):
    """执行超时：归类为瞬时失败，可重试。"""

    code = "timeout"


class TaskCancelled(TaskError):
    """任务被取消：终结态原因。"""

    code = "cancelled"


class SideEffectCommitted(TaskError):
    """不可重复的副作用已生效，但任务整体未跑完。

    与「什么都没发生就失败」（permanent/transient 重试耗尽）可区分：
    该任务绝不再重试，需要人工介入或补偿处理。
    """

    code = "side_effect_committed"


class BudgetExhaustedError(TaskError):
    """整体时间预算用尽：与单次执行超时（timeout）可区分。"""

    code = "budget_exhausted"


class InvalidRequestParameterError(TaskError):
    """提交参数（如重试次数/时间预算覆盖值）超出合法范围。"""

    code = "invalid_request_parameter"


class UnknownTaskTypeError(TaskError):
    """未注册的 task_type：归类为永久失败，重试无意义。"""

    code = "unknown_task_type"
