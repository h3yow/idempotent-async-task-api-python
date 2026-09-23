"""任务处理器注册。

处理器是一个 ``async (payload, context) -> result`` 的可调用对象。
失败分类约定：

- 抛 :class:`~task_api.errors.PermanentTaskError`：永久失败，不再重试；
- 抛 :class:`~task_api.errors.TransientTaskError`：瞬时失败，按退避重试；
- 抛其他 ``Exception``：按 ``settings.unknown_error_is_transient`` 归类。

副作用契约：处理器在**不可重试的外部副作用**（扣款、发消息等）真正发生时，
调用 ``ctx.side_effect()`` 如实上报；框架保证：

- 成功路径结果只提交一次，重复提交同幂等键绝不会再次运行处理器；
- 瞬时失败的重试仅在副作用未提交时安全；一旦 ``side_effect_occurred=True``，
  该次尝试若随后报错将被强制按永久失败处理（避免重复副作用）。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from task_api.errors import HandlerNotFound
from task_api.tracing import get_request_id, get_trace_id


@dataclass(frozen=True)
class HandlerContext:
    """传递给处理器的执行上下文。"""

    task_id: str
    idempotency_key: str
    attempt: int
    max_attempts: int
    trace_id: str
    request_id: str
    side_effect: Callable[[], None] = lambda: None
    is_cancelled: Callable[[], bool] = lambda: False

    @property
    def is_retry(self) -> bool:
        return self.attempt > 1


#: 处理器签名：payload + 上下文 -> 任意可 JSON 序列化结果
TaskHandler = Callable[[dict[str, Any], HandlerContext], Awaitable[Any]]


class HandlerRegistry:
    """按 task_type 注册/查找处理器。"""

    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_type: str, handler: TaskHandler) -> None:
        if not task_type:
            raise ValueError("task_type must not be empty")
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler:
        try:
            return self._handlers[task_type]
        except KeyError:
            raise HandlerNotFound(task_type) from None

    def __contains__(self, task_type: object) -> bool:
        return task_type in self._handlers


def current_context_kwargs() -> dict[str, str]:
    """供处理器内部读取当前链路信息的便捷函数。"""
    return {
        "request_id": get_request_id(),
        "trace_id": get_trace_id(),
    }
