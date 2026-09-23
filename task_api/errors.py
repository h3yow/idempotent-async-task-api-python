"""错误分类与可区分的错误码。

所有面向调用方的失败都携带稳定的字符串错误码（:class:`ErrorCode`），
使"幂等冲突 / 队列已满 / 永久失败 / 瞬时失败 / 重试用尽 / 超时 / 取消"
在任意入口（HTTP 响应、任务状态、日志）下都可区分、可解释。
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """对外暴露的、可区分的失败原因。"""

    # 同一幂等键携带了不同的请求内容
    IDEMPOTENCY_KEY_CONFLICT = "IDEMPOTENCY_KEY_CONFLICT"
    # 等待队列已满，背压拒绝
    QUEUE_FULL = "QUEUE_FULL"
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    TASK_ALREADY_TERMINAL = "TASK_ALREADY_TERMINAL"
    TASK_NOT_CANCELLABLE = "TASK_NOT_CANCELLABLE"
    HANDLER_NOT_FOUND = "HANDLER_NOT_FOUND"
    # 永久失败：业务上不可重试
    PERMANENT_FAILURE = "PERMANENT_FAILURE"
    # 瞬时失败：可以重试
    TRANSIENT_FAILURE = "TRANSIENT_FAILURE"
    # 瞬时失败在最大尝试次数后仍未成功
    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    # 内部一致性保护：出现非法状态流转（正常使用下不应暴露给调用方）
    INVALID_STATE_TRANSITION = "INVALID_STATE_TRANSITION"


class TaskError(Exception):
    """所有业务错误的基类，携带稳定的错误码与说明。"""

    error_code: ErrorCode

    def __init__(self, error_code: ErrorCode, message: str = "") -> None:
        self.error_code = error_code
        self.message = message or error_code.value
        super().__init__(f"[{error_code.value}] {self.message}")


class IdempotencyConflict(TaskError):
    """同一幂等键携带不同请求内容（HTTP 409）。"""

    def __init__(self, message: str = "idempotency key reused with different request") -> None:
        super().__init__(ErrorCode.IDEMPOTENCY_KEY_CONFLICT, message)


class QueueFull(TaskError):
    """排队队列已满，背压拒绝（HTTP 429 / 503）。"""

    def __init__(self, message: str = "task queue is full") -> None:
        super().__init__(ErrorCode.QUEUE_FULL, message)


class TaskNotFound(TaskError):
    """任务不存在。"""

    def __init__(self, message: str = "task not found") -> None:
        super().__init__(ErrorCode.TASK_NOT_FOUND, message)


class TaskNotCancellable(TaskError):
    """任务当前状态不允许取消。"""

    def __init__(self, message: str = "task is not cancellable in its current state") -> None:
        super().__init__(ErrorCode.TASK_NOT_CANCELLABLE, message)


class HandlerNotFound(TaskError):
    """未注册的任务类型。"""

    def __init__(self, task_type: str = "") -> None:
        msg = f"handler not found for task_type={task_type!r}" if task_type else "handler not found"
        super().__init__(ErrorCode.HANDLER_NOT_FOUND, msg)


class PermanentTaskError(TaskError):
    """永久失败：不可重试，任务直接进入 FAILED。"""

    def __init__(self, message: str = "permanent failure") -> None:
        super().__init__(ErrorCode.PERMANENT_FAILURE, message)


class TransientTaskError(TaskError):
    """瞬时失败：可按重试策略再次尝试。"""

    def __init__(self, message: str = "transient failure") -> None:
        super().__init__(ErrorCode.TRANSIENT_FAILURE, message)


class RetriesExhausted(TaskError):
    """瞬时失败在最大重试次数后仍未成功。"""

    def __init__(self, message: str = "retries exhausted") -> None:
        super().__init__(ErrorCode.RETRIES_EXHAUSTED, message)


class TaskTimeout(TaskError):
    """执行超时。按配置可归类为瞬时或永久失败。"""

    def __init__(self, message: str = "execution timeout") -> None:
        super().__init__(ErrorCode.TIMEOUT, message)


class TaskCancelled(TaskError):
    """任务在排队或执行期间被取消。"""

    def __init__(self, message: str = "task cancelled") -> None:
        super().__init__(ErrorCode.CANCELLED, message)
