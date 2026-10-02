"""任务处理器注册表：不同 task_type 的副作用逻辑在此注册。

约定：
- 抛出 TransientError 表示瞬时失败，可重试；
- 抛出 PermanentError 表示永久失败，不重试；
- 处理器自身必须保证副作用幂等（重试不产生重复副作用），
  可通过 context.attempts 与任务 payload 中的业务键判断；
- 对「一旦发生就绝不能再做第二次」的动作，处理器应在动作实际生效后
  立即调用 context.commit_side_effect()：此后任何失败/超时都不会再
  触发重试，任务以 ABORTED 终态提前结束，等待人工或补偿处理。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .models import TaskRecord

TaskHandler = Callable[[TaskRecord, "TaskContext"], Awaitable[Any]]


@dataclass
class TaskContext:
    """传给处理器的执行上下文。"""

    attempts: int            # 当前是第几次尝试（1 表示首次）
    cancelled: bool = False
    _record: TaskRecord | None = None  # 由执行器注入，用于副作用提交标记

    def commit_side_effect(self) -> None:
        """标记「不可重复的副作用已实际生效」。

        必须在动作真正生效（扣款成功、通知已发、外部单据已落）之后调用。
        一旦标记，本次及后续任何失败/超时都不会再触发重试，
        任务以 ABORTED 终态结束，避免同一动作被执行第二次。
        """
        if self._record is not None:
            self._record.side_effect_committed = True


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_type: str, handler: TaskHandler) -> None:
        if not task_type:
            raise ValueError("task_type 不能为空")
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler:
        try:
            return self._handlers[task_type]
        except KeyError:
            from .errors import UnknownTaskTypeError

            raise UnknownTaskTypeError(f"未注册的任务类型: {task_type}") from None
