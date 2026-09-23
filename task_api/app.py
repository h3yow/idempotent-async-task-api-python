"""应用装配入口。

创建默认的存储、处理器注册、执行器、服务与 FastAPI 应用。
测试中可使用 :func:`build_application` 注入自定义 Settings。
"""

from __future__ import annotations

from fastapi import FastAPI

from task_api.api import create_app
from task_api.config import Settings, get_settings
from task_api.errors import (
    PermanentTaskError,
    TaskTimeout,
    TransientTaskError,
)
from task_api.executor import TaskExecutor
from task_api.handlers import HandlerRegistry
from task_api.logging_setup import configure_logging
from task_api.service import TaskService
from task_api.storage import TaskStore


def _register_default_handlers(registry: HandlerRegistry) -> None:
    """注册一组演示处理器，覆盖成功/瞬时/永久/超时/副作用分类。"""

    async def echo(payload, ctx):
        return {"echo": payload, "attempt": ctx.attempt, "task_id": ctx.task_id}

    async def always_transient(payload, ctx):
        raise TransientTaskError("simulated transient failure")

    async def always_permanent(payload, ctx):
        raise PermanentTaskError("simulated permanent failure")

    async def slow(payload, ctx):
        import asyncio

        delay = float(payload.get("delay_seconds", 0.2))
        await asyncio.sleep(delay)
        if ctx.is_cancelled():
            raise TaskTimeout("cancelled before completion")
        return {"slept": delay}

    registry.register("echo", echo)
    registry.register("flaky.transient", always_transient)
    registry.register("flaky.permanent", always_permanent)
    registry.register("slow", slow)


def build_application(
    settings: Settings | None = None,
    *,
    register_demo_handlers: bool = True,
) -> tuple[FastAPI, TaskService]:
    """组装应用。返回 ``(fastapi_app, service)``，测试可直接驱动 service。"""
    configure_logging()
    settings = settings or get_settings()
    store = TaskStore()
    registry = HandlerRegistry()
    if register_demo_handlers:
        _register_default_handlers(registry)
    executor = TaskExecutor(store, registry, settings)
    service = TaskService(store, executor, registry, settings)
    app = create_app(service)
    app.state.service = service
    app.state.store = store
    app.state.executor = executor
    app.state.registry = registry
    return app, service


app, service = build_application()
