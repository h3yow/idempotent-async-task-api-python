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
            TaskState.PENDING,  # 瞬时失败/超时后排队重试
        }
    ),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}


@dataclass
class SubmitRequest:
    """任务提交请求（请求内容指纹参与幂等冲突判定）。"""

    idempotency_key: str
    task_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    request_id: str | None = None  # 请求标识，用于链路追踪/日志
    # 可选覆盖：不指定（None）时完全沿用 Settings 默认行为
    max_retries: int | None = None           # 覆盖最大重试次数
    time_budget_seconds: float | None = None  # 覆盖整体时间预算

    def content_fingerprint(self) -> str:
        """对请求内容（task_type + payload + 覆盖参数）做稳定指纹。

        同一幂等键携带不同指纹 => IdempotencyConflictError。
        request_id 不参与指纹（重放时通常会变）；覆盖参数参与指纹，
        同键但覆盖取值不同视为不同内容，明确拒绝而非静默复用。
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
    # 整体时间预算（秒）；None 表示不设预算（沿用旧行为）
    time_budget_seconds: float | None = None
    # 预算截止时刻（time.monotonic() 基准），仅当 time_budget_seconds 非空时设置
    deadline_monotonic: float | None = None
    # 不可重复的副作用是否已实际生效（生效后绝不再重试）
    side_effect_occurred: bool = False
    result: Any = None
    error_code: str | None = None
    error_message: str | None = None
    failure_kind: str | None = None  # transient / permanent
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    # (时间戳, 请求标识, 原状态, 新状态, 说明)
    history: list[tuple[float, str, TaskState, TaskState, str]] = field(default_factory=list)

    def budget_exhausted(self) -> bool:
        """整体时间预算是否已用尽（未设预算时恒为 False）。"""
        return (
            self.deadline_monotonic is not None
            and time.monotonic() >= self.deadline_monotonic
        )

    def will_retry(self) -> bool:
        """从当前记录推断是否还会发生重试（供查询视图展示）。"""
        if self.state in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED):
            return False
        # 副作用已生效：后续任何失败都会直接终结，不再重试
        return not self.side_effect_occurred

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
