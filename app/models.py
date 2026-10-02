"""领域模型：任务状态机、请求/响应结构。"""

from __future__ import annotations

import enum
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


class TaskState(str, enum.Enum):
    """任务状态机：

    PENDING -> RUNNING -> SUCCEEDED
                       \\-> FAILED (永久失败 / 重试耗尽)
    PENDING/RUNNING -> CANCELLED
    RUNNING 超时/瞬时失败 -> PENDING (排队重试)
    """

    PENDING = "PENDING"        # 已受理，等待并发槽位
    RUNNING = "RUNNING"        # 正在执行（含重试中的某一次尝试）
    SUCCEEDED = "SUCCEEDED"    # 终态：成功
    FAILED = "FAILED"          # 终态：永久失败或重试耗尽
    CANCELLED = "CANCELLED"    # 终态：被取消
    # 终态：不可重复的副作用已实际生效，但任务整体未跑完；
    # 为避免同一动作被执行第二次而提前终止，需人工/补偿介入。
    ABORTED = "ABORTED"


# 允许的状态流转，保证状态在超时/取消/重试间一致可解释
ALLOWED_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset(
        {TaskState.RUNNING, TaskState.CANCELLED, TaskState.FAILED}
    ),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCEEDED,
            TaskState.FAILED,
            TaskState.CANCELLED,
            TaskState.ABORTED,   # 副作用已生效后的失败：不再重试
            TaskState.PENDING,  # 瞬时失败/超时后排队重试
        }
    ),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
    TaskState.ABORTED: frozenset(),
}


#: 终态集合（供执行器/查询层统一判断，避免散落字面量）
TERMINAL_STATES: frozenset[TaskState] = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED, TaskState.ABORTED}
)


@dataclass
class SubmitRequest:
    """任务提交请求（请求内容指纹参与幂等冲突判定）。"""

    idempotency_key: str
    task_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    request_id: str | None = None  # 请求标识，用于链路追踪/日志
    # 可选覆盖：不指定（None）则完全沿用 Settings 默认值
    max_retries: int | None = None          # 最大重试次数（不含首次执行）
    time_budget_seconds: float | None = None  # 整体时间预算（秒）

    def content_fingerprint(self) -> str:
        """对请求内容（task_type + payload + 覆盖参数）做稳定指纹。

        同一幂等键携带不同指纹 => IdempotencyConflictError。
        request_id 不参与指纹（重放时通常会变）。
        覆盖参数参与指纹：同键但覆盖取值不同视为不同内容，明确拒绝，
        避免「同键复用任务却静默忽略新参数」的歧义。
        """
        canonical = json.dumps(
            {
                "task_type": self.task_type,
                "payload": self.payload,
                "max_retries": self.max_retries,
                "time_budget_seconds": self.time_budget_seconds,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class TaskRecord:
    """一个幂等键对应且仅对应一个任务记录。"""

    id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any]
    request_id: str
    fingerprint: str = ""
    state: TaskState = TaskState.PENDING
    attempts: int = 0                 # 已开始的执行次数
    max_retries: int = 0
    # 整体时间预算（秒）；None 表示不限制。deadline_at 为绝对截止时间。
    time_budget_seconds: float | None = None
    deadline_at: float | None = None
    # 不可重复的副作用是否已实际生效（由处理器通过 ctx.commit_side_effect() 标记）。
    # 一旦为 True，任何后续失败都不得再触发重试，任务以 ABORTED 终结。
    side_effect_committed: bool = False
    result: Any = None
    error_code: str | None = None
    error_message: str | None = None
    failure_kind: str | None = None  # transient / permanent / side_effect_committed / budget_exhausted
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    # (时间戳, 请求标识, 原状态, 新状态, 说明)
    history: list[tuple[float, str, TaskState, TaskState, str]] = field(default_factory=list)

    @property
    def will_retry(self) -> bool:
        """是否仍可能发起新的执行尝试（未到达终态）。

        终态（含 ABORTED：副作用已生效提前终止）一律返回 False。
        """
        return self.state not in TERMINAL_STATES

    def transition(self, target: TaskState, note: str = "") -> None:
        """执行一次合法的状态流转；非法流转立即报错（状态机一致性）。"""
        allowed = ALLOWED_TRANSITIONS.get(self.state, frozenset())
        if target not in allowed:
            raise RuntimeError(
                f"非法状态流转: {self.state.value} -> {target.value} "
                f"(task={self.id}, note={note!r})"
            )
        self.history.append((time.time(), self.request_id, self.state, target, note))
        self.state = target
        self.updated_at = time.time()


def new_task_id() -> str:
    return "task_" + uuid.uuid4().hex[:16]
