"""HTTP 请求/响应模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SubmitHttpRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    task_type: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)


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
