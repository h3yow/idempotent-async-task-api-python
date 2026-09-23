"""FastAPI 接口层：提交、查询、取消。

所有响应/错误体都携带 request_id、当前并发度与队列深度，
使重复提交、过载与冲突在 HTTP 入口下同样可解释。
"""

from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .errors import (
    IdempotencyConflictError,
    QueueFullError,
    TaskNotFoundError,
)
from .models import SubmitRequest, TaskRecord
from .schemas import ErrorResponse, StateEvent, SubmitHttpRequest, SubmitResponse, TaskView
from .service import TaskService

logger = logging.getLogger("task_api")


def create_app(service: TaskService) -> FastAPI:
    app = FastAPI(title="幂等异步任务 API", version="0.1.0")

    def _view(record: TaskRecord) -> TaskView:
        return TaskView(
            task_id=record.id,
            idempotency_key=record.idempotency_key,
            task_type=record.task_type,
            state=record.state.value,
            attempts=record.attempts,
            result=record.result,
            error_code=record.error_code,
            error_message=record.error_message,
            failure_kind=record.failure_kind,
            request_id=record.request_id,
            history=[
                StateEvent(
                    at=at,
                    request_id=req_id,
                    from_state=src.value,
                    to_state=dst.value,
                    note=note,
                )
                for at, req_id, src, dst, note in record.history
            ],
        )

    def _capacity() -> tuple[int, int]:
        return service._store.running_count(), service._store.queue_size()

    def _request_id(request: Request) -> str:
        return request.headers.get("x-request-id") or "req-" + uuid.uuid4().hex[:12]

    def _error_body(
        exc: Exception, code: str, message: str, request_id: str
    ) -> ErrorResponse:
        running, queue = _capacity()
        return ErrorResponse(
            error_code=code,
            message=message,
            request_id=request_id,
            current_concurrency=running,
            queue_depth=queue,
        )

    @app.exception_handler(IdempotencyConflictError)
    async def _conflict_handler(
        _request: Request, exc: IdempotencyConflictError
    ) -> JSONResponse:
        body = _error_body(exc, exc.code, str(exc), getattr(exc, "_request_id", "-"))
        logger.warning(
            "幂等冲突 code=%s req=%s concurrency=%s queue=%s msg=%s",
            exc.code,
            body.request_id,
            body.current_concurrency,
            body.queue_depth,
            exc,
        )
        return JSONResponse(status_code=409, content=body.model_dump())

    @app.exception_handler(QueueFullError)
    async def _full_handler(_request: Request, exc: QueueFullError) -> JSONResponse:
        body = _error_body(exc, exc.code, str(exc), getattr(exc, "_request_id", "-"))
        logger.warning(
            "容量拒绝 code=%s req=%s concurrency=%s queue=%s msg=%s",
            exc.code,
            body.request_id,
            body.current_concurrency,
            body.queue_depth,
            exc,
        )
        # 503 + Retry-After：过载是暂时的，客户端可稍后用同一幂等键重放
        return JSONResponse(
            status_code=503,
            content=body.model_dump(),
            headers={"Retry-After": "1"},
        )

    @app.exception_handler(TaskNotFoundError)
    async def _not_found_handler(
        _request: Request, exc: TaskNotFoundError
    ) -> JSONResponse:
        body = _error_body(exc, exc.code, str(exc), "-")
        return JSONResponse(status_code=404, content=body.model_dump())

    @app.post("/tasks", response_model=SubmitResponse, status_code=202)
    async def submit_task(
        body: SubmitHttpRequest, request: Request
    ) -> SubmitResponse:
        request_id = _request_id(request)
        submit = SubmitRequest(
            idempotency_key=body.idempotency_key,
            task_type=body.task_type,
            payload=body.payload,
            request_id=request_id,
        )
        try:
            record, created, reason = await service.submit(submit)
        except (IdempotencyConflictError, QueueFullError) as exc:
            exc._request_id = request_id  # type: ignore[attr-defined]
            raise
        running, queue = _capacity()
        logger.info(
            "提交受理 req=%s key=%s task=%s created=%s reason=%s concurrency=%s queue=%s state=%s",
            request_id,
            body.idempotency_key,
            record.id,
            created,
            reason,
            running,
            queue,
            record.state.value,
        )
        return SubmitResponse(
            created=created,
            reason=reason,
            current_concurrency=running,
            queue_depth=queue,
            task=_view(record),
        )

    @app.get("/tasks/{task_id}", response_model=TaskView)
    async def get_task(task_id: str) -> TaskView:
        return _view(await service.get(task_id))

    @app.post("/tasks/{task_id}/cancel", response_model=TaskView)
    async def cancel_task(task_id: str) -> TaskView:
        record = await service.cancel(task_id)
        logger.info(
            "任务取消 task=%s req=%s state=%s",
            task_id,
            record.request_id,
            record.state.value,
        )
        return _view(record)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
