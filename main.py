"""本地启动入口：uvicorn main:app

幂等异步任务提交与执行服务。
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import create_app
from app.wiring import build_service

service, executor, _store = build_service()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await executor.start()
    try:
        yield
    finally:
        await executor.stop()


app: FastAPI = create_app(service)
app.router.lifespan_context = lifespan
