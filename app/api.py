"""FastAPI 接口层：提交、查询、取消。

所有响应/错误体都携带 request_id、当前并发度与队列深度，
使重复提交、过载与冲突在 HTTP 入口下同样可解释。
"""

from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .errors import (
    IdempotencyConflictError,
    InvalidParameterError,
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
            max_retries=record.max_retries,
            time_budget_seconds=record.time_budget_seconds,
            side_effect_committed=record.side_effect_committed,
            will_retry=record.will_retry,
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

    @app.exception_handler(InvalidParameterError)
    async def _invalid_param_handler(
        _request: Request, exc: InvalidParameterError
    ) -> JSONResponse:
        body = _error_body(exc, exc.code, str(exc), getattr(exc, "_request_id", "-"))
        logger.warning(
            "参数拒绝 code=%s req=%s concurrency=%s queue=%s msg=%s",
            exc.code,
            body.request_id,
            body.current_concurrency,
            body.queue_depth,
            exc,
        )
        return JSONResponse(status_code=400, content=body.model_dump())

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """请求体结构/类型非法：与参数范围校验统一为 400 invalid_parameter。"""
        details = "; ".join(
            f"{'.'.join(str(p) for p in e.get('loc', []))}: {e.get('msg', '')}"
            for e in exc.errors()
        )
        body = _error_body(
            exc, "invalid_parameter", f"请求参数非法: {details}", _request_id(request)
        )
        logger.warning(
            "请求校验拒绝 req=%s concurrency=%s queue=%s msg=%s",
            body.request_id,
            body.current_concurrency,
            body.queue_depth,
            details,
        )
        return JSONResponse(status_code=400, content=body.model_dump())

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
            max_retries=body.max_retries,
            time_budget_seconds=body.time_budget_seconds,
        )
        try:
            record, created, reason = await service.submit(submit)
        except (IdempotencyConflictError, QueueFullError, InvalidParameterError) as exc:
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

    @app.get("/tasks/by-key/{idempotency_key}", response_model=TaskView)
    async def get_task_by_key(idempotency_key: str, request: Request) -> TaskView:
        """按幂等键反查任务：纯查询，不触发执行或副作用。

        查不到时返回 404 task_not_found（可区分，绝不 500/空响应）。
        """
        record = await service.get_by_key(idempotency_key)
        logger.info(
            "幂等键反查 req=%s key=%s task=%s state=%s attempts=%s side_effect=%s will_retry=%s",
            _request_id(request),
            idempotency_key,
            record.id,
            record.state.value,
            record.attempts,
            record.side_effect_committed,
            record.will_retry,
        )
        return _view(record)

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
