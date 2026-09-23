"""FastAPI 路由、错误映射与请求/链路标识中间件。

所有业务失败都映射为可区分的 HTTP 状态码与稳定错误码：

==============================================  =====================
错误码                                           HTTP 状态
==============================================  =====================
IDEMPOTENCY_KEY_CONFLICT                         409
QUEUE_FULL                                       503 (Retry-After)
TASK_NOT_FOUND                                   404
TASK_ALREADY_TERMINAL / TASK_NOT_CANCELLABLE     409
HANDLER_NOT_FOUND                                422
PERMANENT_FAILURE / RETRIES_EXHAUSTED            200 (体现在任务状态)
TIMEOUT / CANCELLED                              200 (体现在任务状态)
==============================================  =====================
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from task_api.errors import (
    ErrorCode,
    TaskError,
)
from task_api.models import CancelResponse, SubmitRequest, TaskView
from task_api.service import TaskService
from task_api.tracing import resolve_request_id, resolve_trace_id

_HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.IDEMPOTENCY_KEY_CONFLICT: 409,
    ErrorCode.QUEUE_FULL: 503,
    ErrorCode.TASK_NOT_FOUND: 404,
    ErrorCode.TASK_ALREADY_TERMINAL: 409,
    ErrorCode.TASK_NOT_CANCELLABLE: 409,
    ErrorCode.HANDLER_NOT_FOUND: 422,
}


def _error_response(exc: TaskError) -> JSONResponse:
    status_code = _HTTP_STATUS.get(exc.error_code, 400)
    headers = {}
    if exc.error_code == ErrorCode.QUEUE_FULL:
        headers["Retry-After"] = "1"
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": exc.error_code.value,
                "message": exc.message,
            }
        },
        headers=headers,
    )


def create_app(service: TaskService) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            await service.shutdown()

    app = FastAPI(
        title="Idempotent Async Task API",
        version="0.1.0",
        description="带幂等控制、并发限流与失败分类的异步任务提交与执行接口",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def identity_middleware(request: Request, call_next):
        # 透传/生成请求标识与链路标识，写入响应头，贯穿后续日志与状态事件
        request_id = resolve_request_id(request.headers.get("x-request-id"))
        trace_id = resolve_trace_id(
            request.headers.get("x-trace-id"), request_id=request_id
        )
        request.state.request_id = request_id
        request.state.trace_id = trace_id
        try:
            response: Response = await call_next(request)
        except Exception:
            raise
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Trace-ID"] = trace_id
        return response

    @app.exception_handler(TaskError)
    async def task_error_handler(request: Request, exc: TaskError):
        return _error_response(exc)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/tasks", response_model=TaskView, status_code=202)
    async def submit_task(request: Request, body: SubmitRequest) -> TaskView:
        return await service.submit(
            body,
            request_id=request.state.request_id,
            trace_id=request.state.trace_id,
        )

    @app.get("/tasks/{task_id}", response_model=TaskView)
    async def get_task(task_id: str) -> TaskView:
        return await service.get_task(task_id)

    @app.get("/idempotency/{idempotency_key}", response_model=TaskView)
    async def get_by_idempotency_key(idempotency_key: str) -> TaskView:
        return await service.get_by_idempotency_key(idempotency_key)

    @app.post("/tasks/{task_id}/cancel", response_model=CancelResponse)
    async def cancel_task(task_id: str) -> CancelResponse:
        return await service.cancel(task_id)

    return app
