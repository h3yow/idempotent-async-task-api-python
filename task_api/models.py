"""核心领域模型：任务状态、失败分类、状态流转事件与请求/响应模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from task_api.errors import ErrorCode


def utcnow() -> datetime:
    """返回带时区的 UTC 当前时间。"""
    return datetime.now(timezone.utc)


class TaskStatus(str, Enum):
    """任务状态机。

    合法流转（由 :data:`ALLOWED_TRANSITIONS` 强制校验）::

        PENDING   -> QUEUED | CANCELLED | FAILED
        QUEUED    -> RUNNING | CANCELLED
        RUNNING   -> SUCCEEDED | FAILED | RETRYING | CANCELLED
        RETRYING  -> QUEUED | CANCELLED | FAILED
        SUCCEEDED / FAILED / CANCELLED 为终态，不再流转

    语义说明：
    - RETRYING：瞬时失败后处于退避等待，尚未重新排队；
    - 超时按配置分类：瞬时超时回到 RETRYING，永久超时进入 FAILED；
    - 取消在 PENDING/QUEUED/RETRYING 下确定性生效；RUNNING 下取消属于
      协作式取消（处理器需响应取消信号），生效后进入 CANCELLED。
    """

    PENDING = "PENDING"        # 已受理、尚未进入队列
    QUEUED = "QUEUED"          # 在有界队列中等待执行额度
    RUNNING = "RUNNING"        # 已获得并发额度，正在执行处理器
    RETRYING = "RETRYING"      # 瞬时失败，退避等待重新排队
    SUCCEEDED = "SUCCEEDED"    # 终态：成功（副作用最多发生一次）
    FAILED = "FAILED"          # 终态：永久失败或重试用尽
    CANCELLED = "CANCELLED"    # 终态：被取消

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}
)

#: 允许的状态流转表；任何不在表中的流转都会被存储层拒绝。
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.QUEUED, TaskStatus.CANCELLED, TaskStatus.FAILED}
    ),
    TaskStatus.QUEUED: frozenset(
        {TaskStatus.RUNNING, TaskStatus.CANCELLED}
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.RETRYING,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.RETRYING: frozenset(
        {TaskStatus.QUEUED, TaskStatus.CANCELLED, TaskStatus.FAILED}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


class FailureClass(str, Enum):
    """失败分类：瞬时 vs 永久。"""

    NONE = "NONE"
    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"


class TransitionEvent(BaseModel):
    """一次状态流转的可追踪记录。"""

    seq: int
    from_status: TaskStatus | None
    to_status: TaskStatus
    at: datetime
    attempt: int = 0
    reason: str = ""
    error_code: ErrorCode | None = None
    trace_id: str = ""
    request_id: str = ""


@dataclass
class TaskRecord:
    """任务的内部权威记录（保存在内存存储中）。"""

    task_id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any]
    request_hash: str
    request_id: str
    trace_id: str
    status: TaskStatus = TaskStatus.PENDING
    attempt: int = 0
    max_attempts: int = 1
    failure_class: FailureClass = FailureClass.NONE
    result: Any = None
    error_code: ErrorCode | None = None
    error_message: str = ""
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    history: list[TransitionEvent] = field(default_factory=list)
    side_effect_occurred: bool = False
    enqueue_seq: int = 0
    timeout_seconds: float | None = None

    def to_view(self, queued_position: int | None = None) -> TaskView:
        """生成对外视图。"""
        return TaskView(
            task_id=self.task_id,
            idempotency_key=self.idempotency_key,
            task_type=self.task_type,
            status=self.status,
            attempt=self.attempt,
            max_attempts=self.max_attempts,
            failure_class=self.failure_class,
            result=self.result,
            error_code=self.error_code,
            error_message=self.error_message,
            request_id=self.request_id,
            trace_id=self.trace_id,
            created_at=self.created_at,
            updated_at=self.updated_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            history=list(self.history),
            side_effect_occurred=self.side_effect_occurred,
            queued_position=queued_position,
        )


class SubmitRequest(BaseModel):
    """提交任务请求。"""

    task_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(..., min_length=1)
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    timeout_seconds: float | None = Field(default=None, gt=0)


class TaskView(BaseModel):
    """对外的任务视图。"""

    task_id: str
    idempotency_key: str
    task_type: str
    status: TaskStatus
    attempt: int
    max_attempts: int
    failure_class: FailureClass
    result: Any = None
    error_code: ErrorCode | None = None
    error_message: str = ""
    request_id: str
    trace_id: str
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    history: list[TransitionEvent] = Field(default_factory=list)
    side_effect_occurred: bool = False
    queued_position: int | None = None


class CancelResponse(BaseModel):
    task_id: str
    status: TaskStatus
    cancelled: bool
