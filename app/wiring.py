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


# 演示用「外部账本」：进程内计数，模拟扣款等不可重复动作已对外生效的次数
_charge_ledger: dict[str, int] = {}


async def charge_handler(record: TaskRecord, ctx: TaskContext) -> dict[str, object]:
    """演示处理器：模拟「扣款已生效后收尾失败」的敏感场景。

    payload:
      fail_after_charge: 为 True 时，扣款生效后抛 TransientError。
      permanent_after_charge: 为 True 时，扣款生效后抛 PermanentError
          （明确不可重试的失败）。无论哪种失败类型，只要扣款已生效，
          执行器都以 side_effect_committed 终结、绝不重试，
          且 _charge_ledger 中该任务的扣款次数恒为 1。
    """
    key = record.idempotency_key
    _charge_ledger[key] = _charge_ledger.get(key, 0) + 1
    # 扣款动作已实际生效：立即登记保护标记，此后任何失败都不再重试
    ctx.mark_side_effect()
    if record.payload.get("permanent_after_charge"):
        raise PermanentError(f"扣款后永久收尾失败 (attempt={ctx.attempts})")
    if record.payload.get("fail_after_charge"):
        raise TransientError(f"扣款后收尾失败 (attempt={ctx.attempts})")
    return {"charged": True, "charge_count": _charge_ledger[key]}


def charge_count(idempotency_key: str) -> int:
    """查询演示账本中某幂等键的扣款次数（本地验证用）。"""
    return _charge_ledger.get(idempotency_key, 0)


async def sleep_ok_handler(record: TaskRecord, ctx: TaskContext) -> str:
    """演示处理器：睡眠 payload.sleep 秒后成功（默认 0.2s），无副作用。

    配合极小的 time_budget_seconds 可观察「预算在执行进行中到点」：
    即使处理器再睡一会就会成功，预算到点也立即中止为 budget_exhausted。
    """
    import asyncio

    await asyncio.sleep(float(record.payload.get("sleep", 0.2)))
    return f"ok after sleep (attempt={ctx.attempts})"


def build_handlers() -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register("echo", echo_handler)
    registry.register("flaky", flaky_handler)
    registry.register("charge", charge_handler)
    registry.register("sleep_ok", sleep_ok_handler)
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
