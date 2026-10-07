"""测试用处理器与副作用记录器：

- side_effect 处理器每次成功执行都会向共享记录器追加一次 (task_id, attempt)；
  以此断言“重复提交不重复执行、重试不产生重复副作用”。
- 瞬时/永久失败由 payload 控制；slow 处理器用于占住并发槽位。
"""

from __future__ import annotations

import asyncio
from typing import Any

from app.errors import PermanentError, TransientError
from app.handlers import HandlerRegistry, TaskContext
from app.models import TaskRecord


class EffectLog:
    """跨提交/重试共享的副作用记录（模拟外部系统）。"""

    def __init__(self) -> None:
        self.effects: list[tuple[str, int]] = []

    def add(self, task_id: str, attempt: int) -> None:
        self.effects.append((task_id, attempt))

    def count(self, task_id: str) -> int:
        return sum(1 for tid, _ in self.effects if tid == task_id)

    def attempts(self, task_id: str) -> list[int]:
        return [att for tid, att in self.effects if tid == task_id]

    def reset(self) -> None:
        self.effects.clear()


async def side_effect_handler(record: TaskRecord, ctx: TaskContext) -> dict[str, Any]:
    if record.payload.get("permanent"):
        # 永久失败：在产生任何副作用之前判定，绝不重试
        raise PermanentError("模拟永久失败：不重试")
    EffectLogRef.log.add(record.id, ctx.attempts)
    fail_times = int(record.payload.get("fail_times", 0))
    if ctx.attempts <= fail_times:
        raise TransientError(f"模拟瞬时失败 (attempt={ctx.attempts})")
    return {"attempt": ctx.attempts, "echo": record.payload.get("value")}


async def flaky_handler(record: TaskRecord, ctx: TaskContext) -> str:
    if record.payload.get("permanent"):
        raise PermanentError("模拟永久失败：不重试")
    fail_times = int(record.payload.get("fail_times", 0))
    if ctx.attempts <= fail_times:
        raise TransientError(f"模拟瞬时失败 (attempt={ctx.attempts})")
    return f"ok after {ctx.attempts} attempt(s)"


async def slow_handler(record: TaskRecord, ctx: TaskContext) -> str:
    """占用槽位直到 hold event 被置位（测试并发上限/排队）。"""
    holder = SlowRef.holders[record.idempotency_key]
    try:
        await asyncio.wait_for(holder.wait(), timeout=record.payload.get("max_wait", 10.0))
    except asyncio.TimeoutError:
        raise TransientError("slow handler 等待超时")
    EffectLogRef.log.add(record.id, ctx.attempts)
    return f"slow-done attempt={ctx.attempts}"


class EffectLogRef:
    """由 conftest 在夹具中注入当前 EffectLog。"""

    log: EffectLog = EffectLog()


class SlowRef:
    holders: dict[str, asyncio.Event] = {}


def install_test_handlers(registry: HandlerRegistry, effects: EffectLog) -> None:
    EffectLogRef.log = effects
    registry.register("side_effect", side_effect_handler)
    registry.register("flaky", flaky_handler)
    registry.register("slow", slow_handler)
    registry.register("sleepy", sleepy_handler)
    registry.register("timeout_then_ok", timeout_then_ok_handler)
    registry.register("sensitive", sensitive_handler)


def make_holder(key: str) -> asyncio.Event:
    event = asyncio.Event()
    SlowRef.holders[key] = event
    return event


def release_holder(key: str) -> None:
    SlowRef.holders[key].set()


async def sleepy_handler(record: TaskRecord, ctx: TaskContext) -> dict[str, Any]:
    """睡 payload['sleep'] 秒后成功：用于验证整体预算在执行中途的硬打断。

    只有真正睡满并返回才会记录副作用；被预算/超时打断时记录数为 0。
    """
    slept = float(record.payload.get("sleep", 0.1))
    await asyncio.sleep(slept)
    EffectLogRef.log.add(record.id, ctx.attempts)
    return {"slept": slept, "attempt": ctx.attempts}


async def timeout_then_ok_handler(record: TaskRecord, ctx: TaskContext) -> str:
    """前两次尝试睡过超时阈值，第三次立即成功：验证超时按瞬时失败重试。"""
    import asyncio as _asyncio

    if ctx.attempts <= 2:
        await _asyncio.sleep(record.payload.get("sleep", 0.5))
    return f"ok after {ctx.attempts} attempts"


async def sensitive_handler(record: TaskRecord, ctx: TaskContext) -> dict[str, Any]:
    """模拟「不可重复动作」场景：副作用生效后登记保护标记。

    payload:
      fail_before_effect: 前 N 次尝试在副作用生效*之前*抛 TransientError
                          （这些尝试应正常重试，不产生副作用）；
      fail_after_effect: 副作用生效后抛 TransientError（应终结，不重试）；
      permanent_after_effect: 副作用生效后抛 PermanentError
                          （同样应以 side_effect_committed 终结，而非普通永久失败）；
      sleep_after_effect: 副作用生效后睡眠 N 秒（用于触发单次超时）。
    """
    fail_before = int(record.payload.get("fail_before_effect", 0))
    if ctx.attempts <= fail_before:
        raise TransientError(f"副作用生效前的瞬时失败 (attempt={ctx.attempts})")
    # 不可重复动作在此实际生效（扣款/发通知/落单据）
    EffectLogRef.log.add(record.id, ctx.attempts)
    ctx.mark_side_effect()
    sleep_for = float(record.payload.get("sleep_after_effect", 0))
    if sleep_for > 0:
        await asyncio.sleep(sleep_for)
    if record.payload.get("permanent_after_effect"):
        raise PermanentError(f"副作用生效后的永久失败 (attempt={ctx.attempts})")
    if record.payload.get("fail_after_effect"):
        raise TransientError(f"副作用生效后收尾失败 (attempt={ctx.attempts})")
    return {"attempt": ctx.attempts, "charged": True}
