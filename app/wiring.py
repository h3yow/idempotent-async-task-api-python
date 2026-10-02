"""装配默认服务实例与演示用处理器。"""

from __future__ import annotations

from .config import Settings
from .errors import PermanentError, TransientError
from .executor import TaskExecutor
from .handlers import HandlerRegistry, TaskContext
from .models import TaskRecord
from .service import TaskService
from .store import TaskStore


async def echo_handler(record: TaskRecord, ctx: TaskContext) -> dict[str, object]:
    """演示处理器：直接回显 payload（天然幂等，无外部副作用）。"""
    return {"attempt": ctx.attempts, "payload": record.payload}


async def flaky_handler(record: TaskRecord, ctx: TaskContext) -> str:
    """演示处理器：按 payload 配置模拟瞬时/永久失败，便于本地验证重试。

    payload:
      fail_times: 前 N 次尝试抛 TransientError（默认 0）
      permanent:   为 True 时直接抛 PermanentError（默认 False）
    """
    if record.payload.get("permanent"):
        raise PermanentError("配置为永久失败：不可重试")
    fail_times = int(record.payload.get("fail_times", 0))
    if ctx.attempts <= fail_times:
        raise TransientError(f"模拟瞬时失败 (attempt={ctx.attempts})")
    return f"ok after {ctx.attempts} attempt(s)"


async def charge_once_handler(record: TaskRecord, ctx: TaskContext) -> dict[str, object]:
    """演示处理器：模拟「扣款类」不可重复动作的副作用保护。

    动作生效后立即 commit_side_effect()，随后按 payload 模拟失败：
      after_commit: ok（默认）| transient | timeout
    失败时任务以 ABORTED 终结而不是重试，可用
    GET /tasks/by-key/{key} 观察到 side_effect_committed=true。
    """
    import asyncio

    # —— 不可重复的动作在此生效（演示：仅记录日志）——
    ctx.commit_side_effect()
    mode = record.payload.get("after_commit", "ok")
    if mode == "transient":
        raise TransientError("扣款已生效，但后续步骤瞬时失败：不再重试")
    if mode == "timeout":
        await asyncio.sleep(float(record.payload.get("sleep", 30.0)))
    return {"charged": True, "attempt": ctx.attempts}


def build_handlers() -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register("echo", echo_handler)
    registry.register("flaky", flaky_handler)
    registry.register("charge_once", charge_once_handler)
    return registry


def default_logger(message: str) -> None:
    import logging

    logging.getLogger("task_executor").info(message)


def build_service(
    settings: Settings | None = None,
    handlers: HandlerRegistry | None = None,
    log=None,
) -> tuple[TaskService, TaskExecutor, TaskStore]:
    """装配 (service, executor, store)；executor 需由调用方 start/stop。"""
    settings = settings or Settings()
    handlers = handlers or build_handlers()
    store = TaskStore(settings)
    executor = TaskExecutor(store, handlers, settings, log=log or default_logger)
    service = TaskService(store, executor, settings)
    return service, executor, store
