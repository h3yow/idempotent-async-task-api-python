"""任务处理器注册表：不同 task_type 的副作用逻辑在此注册。

约定：
- 抛出 TransientError 表示瞬时失败，可重试；
- 抛出 PermanentError 表示永久失败，不重试；
- 处理器自身必须保证副作用幂等（重试不产生重复副作用），
  可通过 context.attempts 与任务 payload 中的业务键判断；
- 对「一旦生效就绝不能做第二次」的动作（扣款/发通知/落外部单据），
  处理器在动作实际生效后调用 context.mark_side_effect()：
  此后无论再遇到瞬时失败还是超时，执行器都会让任务以
  side_effect_committed 原因终结，绝不通过重试重复该动作。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .models import TaskRecord

TaskHandler = Callable[[TaskRecord, "TaskContext"], Awaitable[Any]]


@dataclass
class TaskContext:
    """传给处理器的执行上下文。"""

    attempts: int            # 当前是第几次尝试（1 表示首次）
    cancelled: bool = False
    # 由执行器注入：把「副作用已生效」标记写回任务记录
    _side_effect_marker: Callable[[], None] | None = field(
        default=None, repr=False
    )

    def mark_side_effect(self) -> None:
        """声明不可重复的副作用已实际生效。

        生效标记落库后，本次尝试若再失败（含超时），任务将以
        side_effect_committed 终结而不是重试，保证同一动作不被做第二遍。
        """
        if self._side_effect_marker is not None:
            self._side_effect_marker()


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
