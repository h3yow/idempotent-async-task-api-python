"""测试夹具：装配隔离的 service / executor / registry，并打开日志。

单测日志会打印请求标识、链路标识、并发度与状态流转（见 task_api.logging_setup）。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

import pytest

from task_api.config import Settings
from task_api.errors import (
    PermanentTaskError,
    TransientTaskError,
)
from task_api.executor import TaskExecutor
from task_api.handlers import HandlerRegistry
from task_api.logging_setup import configure_logging
from task_api.models import SubmitRequest
from task_api.service import TaskService
from task_api.storage import TaskStore


@pytest.fixture(scope="session", autouse=True)
def _configure_test_logging() -> None:
    configure_logging(logging.INFO, force=True)


def make_settings(**overrides) -> Settings:
    defaults = dict(
        max_concurrency=2,
        queue_capacity=4,
        default_max_attempts=3,
        retry_backoff_base_seconds=0.01,
        retry_backoff_max_seconds=0.1,
        default_timeout_seconds=1.0,
        total_timeout_seconds=None,
        timeout_is_transient=True,
        unknown_error_is_transient=True,
    )
    defaults.update(overrides)
    return Settings(**defaults)


class Harness:
    """一组装配好的组件 + 便捷提交方法。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = TaskStore()
        self.registry = HandlerRegistry()
        self.executor = TaskExecutor(self.store, self.registry, settings)
        self.service = TaskService(
            self.store, self.executor, self.registry, settings
        )
        self.calls: list[str] = []          # 处理器调用记录（验证无重复副作用）
        self.side_effects: list[str] = []   # 副作用提交记录
        self.attempt_count: dict[str, int] = {}
        self._gates: dict[str, asyncio.Event] = {}
        self._register_builtin_handlers()

    def gate(self, name: str) -> asyncio.Event:
        return self._gates.setdefault(name, asyncio.Event())

    def _register_builtin_handlers(self) -> None:
        async def echo(payload, ctx):
            self.calls.append(f"echo:{ctx.task_id}:{ctx.attempt}")
            return {"ok": True, "attempt": ctx.attempt}

        async def transient_then_ok(payload, ctx):
            key = payload.get("key", ctx.task_id)
            self.attempt_count[key] = self.attempt_count.get(key, 0) + 1
            self.calls.append(f"transient_then_ok:{key}:{ctx.attempt}")
            succeed_on = int(payload.get("succeed_on", 2))
            if ctx.attempt < succeed_on:
                raise TransientTaskError(f"transient boom #{ctx.attempt}")
            return {"recovered_on": ctx.attempt}

        async def always_transient(payload, ctx):
            key = payload.get("key", ctx.task_id)
            self.attempt_count[key] = self.attempt_count.get(key, 0) + 1
            self.calls.append(f"always_transient:{key}:{ctx.attempt}")
            raise TransientTaskError("always transient")

        async def always_permanent(payload, ctx):
            key = payload.get("key", ctx.task_id)
            self.calls.append(f"always_permanent:{key}:{ctx.attempt}")
            raise PermanentTaskError("permanent, do not retry")

        async def blocking(payload, ctx):
            name = payload.get("gate", "default")
            self.calls.append(f"blocking:{name}:{ctx.attempt}:start")
            await self.gate(name).wait()
            if ctx.is_cancelled():
                raise RuntimeError("cancelled")
            self.calls.append(f"blocking:{name}:{ctx.attempt}:end")
            return {"gate": name}

        async def side_effect_then_fail(payload, ctx):
            # 第一次：提交副作用后再瞬时失败 -> 必须禁止重试（防重复副作用）
            self.attempt_count[ctx.task_id] = self.attempt_count.get(ctx.task_id, 0) + 1
            ctx.side_effect()
            self.side_effects.append(f"{ctx.task_id}:{ctx.attempt}")
            await asyncio.sleep(0)  # 让 side_effect 标记落库
            raise TransientTaskError("failed after side effect")

        self.registry.register("echo", echo)
        self.registry.register("transient_then_ok", transient_then_ok)
        self.registry.register("always_transient", always_transient)
        self.registry.register("always_permanent", always_permanent)
        self.registry.register("blocking", blocking)
        self.registry.register("side_effect_then_fail", side_effect_then_fail)

    def submit(self, task_type: str, key: str, payload=None, **kw):
        from task_api.tracing import new_id

        body = SubmitRequest(
            task_type=task_type,
            payload=payload or {},
            idempotency_key=key,
            **kw,
        )
        return self.service.submit(
            body, request_id=new_id("req"), trace_id=new_id("trace")
        )

    async def wait(self, task_id: str, timeout: float = 3.0):
        await self.executor.wait_terminal(task_id, timeout=timeout)
        return await self.store.get(task_id)

    async def shutdown(self) -> None:
        for gate in self._gates.values():
            gate.set()
        await self.service.shutdown()


@pytest.fixture
async def harness() -> AsyncIterator[Harness]:
    h = Harness(make_settings())
    try:
        yield h
    finally:
        await h.shutdown()
