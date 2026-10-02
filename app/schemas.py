"""HTTP 请求/响应模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SubmitHttpRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    task_type: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    # 可选覆盖：不指定（缺省/null）则完全沿用服务端默认行为
    max_retries: int | None = None
    time_budget_seconds: float | None = None


class StateEvent(BaseModel):
    at: float
    request_id: str
    from_state: str
    to_state: str
    note: str


class TaskView(BaseModel):
    task_id: str
    idempotency_key: str
    task_type: str
    state: str
    attempts: int
    max_retries: int
    time_budget_seconds: float | None = None
    # 不可重复的副作用是否已实际生效（True 后绝不会再重试）
    side_effect_committed: bool = False
    # 是否仍可能发起新的执行尝试（终态一律为 False）
    will_retry: bool = True
    result: Any = None
    error_code: str | None = None
    error_message: str | None = None
    failure_kind: str | None = None
    request_id: str
    history: list[StateEvent] = Field(default_factory=list)


class SubmitResponse(BaseModel):
    created: bool
    reason: str  # created / duplicate_replay / queued
    current_concurrency: int
    queue_depth: int
    task: TaskView


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    request_id: str
    current_concurrency: int
    queue_depth: int
