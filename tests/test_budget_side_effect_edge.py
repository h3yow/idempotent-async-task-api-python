"""边界场景：整体时间预算是真正的整体上界、副作用保护覆盖所有失败类型。

覆盖：
- 执行进行中跨过预算截止时刻（哪怕本次执行眼看就要成功）也必须立即中止，
  以 budget_exhausted 终结，与单次执行超时 timeout 清晰区分；
- 不可重复动作生效后抛「不可重试的永久失败」，仍以 side_effect_committed
  终结且动作只发生一次；
- 副作用已生效 + 预算到点同时成立时，side_effect_committed 优先；
- 这类提前终结正常归还并发名额，排队任务补位；
- 不传覆盖参数时默认逻辑不变；幂等重放复用、同键异内容拒绝不回归。
"""

from __future__ import annotations

import asyncio
import logging
import time

import pytest

from app.errors import IdempotencyConflictError
from app.models import SubmitRequest, TaskState

from .handlers_helpers import make_holder

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_budget_deadline_mid_attempt_aborts_even_if_success_imminent(components):
    """预算在执行进行中到点：立即中止，哪怕处理器再睡一会就会成功返回。"""
    svc = components.service
    started = time.monotonic()
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="bd-1", task_type="sleep_ok",
        # 单次超时 1.0s：本次执行远不会触发单次超时；
        # 但整体预算 0.15s 先到点，处理器 0.35s 后本会成功返回
        payload={"sleep": 0.35}, request_id="req-bd1",
        max_retries=5, time_budget_seconds=0.15,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    elapsed = time.monotonic() - started

    assert rec.state is TaskState.FAILED
    assert rec.error_code == "budget_exhausted"
    assert rec.failure_kind == "budget_exhausted"
    # 与「单次执行超时」清晰区分
    assert rec.error_code != "timeout"
    assert rec.result is None
    # 只发起过一次尝试，且在预算点附近就被中止，没有放任处理器跑完
    assert rec.attempts == 1
    assert rec.side_effect_occurred is False
    assert elapsed < 0.35, (
        f"预算未兜住执行时长：实际耗时 {elapsed:.3f}s，处理器本会睡满 0.35s"
    )
    logger.info(
        "预算到点 req=req-bd1 attempts=%s elapsed=%.3fs code=%s -> 即使本可成功也中止",
        rec.attempts, elapsed, rec.error_code,
    )


@pytest.mark.asyncio
async def test_budget_deadline_mid_attempt_distinct_from_single_timeout():
    """同样的中止形态：预算到点报 budget_exhausted，单次超时仍报 timeout。"""
    from app.config import Settings
    from app.handlers import HandlerRegistry
    from app.wiring import build_service

    from .handlers_helpers import EffectLog, install_test_handlers

    settings = Settings(
        max_concurrency=1, max_queue_size=4, task_timeout_seconds=0.2,
        max_retries=0, retry_backoff_seconds=0.0,
    )
    handlers = HandlerRegistry()
    install_test_handlers(handlers, EffectLog())
    service, executor, _ = build_service(
        settings, handlers=handlers,
        log=lambda m: logger.info("    | %s", m))
    await executor.start()
    try:
        # 无预算、单次超时：error_code=timeout
        rec_t, _, _ = await service.submit(SubmitRequest(
            idempotency_key="bd-timeout", task_type="sleep_ok",
            payload={"sleep": 1.0}, request_id="req-bdt",
        ))
        rec_t = await service.wait_until_terminal(rec_t.id, timeout=5)
        assert rec_t.error_code == "timeout"

        # 有预算且预算先到点：error_code=budget_exhausted
        rec_b, _, _ = await service.submit(SubmitRequest(
            idempotency_key="bd-budget", task_type="sleep_ok",
            payload={"sleep": 1.0}, request_id="req-bdb",
            time_budget_seconds=0.1,
        ))
        rec_b = await service.wait_until_terminal(rec_b.id, timeout=5)
        assert rec_b.state is TaskState.FAILED
        assert rec_b.error_code == "budget_exhausted"
        assert rec_b.failure_kind == "budget_exhausted"
        logger.info(
            "原因区分 req=req-bdt code=%s；req=req-bdb code=%s -> 清晰可区分",
            rec_t.error_code, rec_b.error_code,
        )
    finally:
        await executor.stop()


@pytest.mark.asyncio
async def test_side_effect_then_permanent_failure_uses_dedicated_reason(components):
    """副作用生效后抛明确不可重试的永久失败：仍以 side_effect_committed 终结。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="bd-2", task_type="sensitive",
        payload={"permanent_after_effect": True}, request_id="req-bd2",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 1
    assert rec.side_effect_occurred is True
    assert rec.failure_kind == "side_effect_committed"
    assert rec.error_code == "side_effect_committed"
    # error_message 中保留原始失败原因，便于人工/补偿定位
    assert "permanent_failure" in (rec.error_message or "")
    assert rec.will_retry() is False
    # 绝不再执行一次：不可重复的动作只发生一次
    assert components.effects.count(rec.id) == 1
    # 与「什么都没发生就永久失败」可区分
    assert rec.failure_kind != "permanent"
    logger.info(
        "生效后永久失败 req=req-bd2 attempts=%s effects=%s kind=%s -> 专用原因",
        rec.attempts, components.effects.count(rec.id), rec.failure_kind,
    )


@pytest.mark.asyncio
async def test_side_effect_occurred_takes_priority_over_budget(components):
    """动作已生效且预算在同一次尝试中到点：side_effect_committed 优先，不重跑。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="bd-3", task_type="sensitive",
        # 标记生效后睡眠，跨过预算 0.15s（但不到单次超时 1.0s）
        payload={"sleep_after_effect": 0.35}, request_id="req-bd3",
        max_retries=5, time_budget_seconds=0.15,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 1
    assert rec.side_effect_occurred is True
    assert rec.error_code == "side_effect_committed"
    assert rec.failure_kind == "side_effect_committed"
    assert "budget_exhausted" in (rec.error_message or "")
    assert components.effects.count(rec.id) == 1
    logger.info(
        "双重边界 req=req-bd3 effects=%s code=%s -> 副作用优先，动作不重复",
        components.effects.count(rec.id), rec.error_code,
    )


@pytest.mark.asyncio
async def test_budget_termination_releases_slot_for_queued(components):
    """预算到点提前终结后：并发名额正常归还，排队任务补位，不漏还不卡住。"""
    svc = components.service
    h0 = make_holder("bdg-0")
    h2 = make_holder("bdg-2")
    r_budget = None
    try:
        r0, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="bdg-0", task_type="slow", payload={},
            request_id="req-bdg0"))
        r_budget, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="bdg-1", task_type="sleep_ok",
            payload={"sleep": 0.35}, request_id="req-bdg1",
            time_budget_seconds=0.15,
        ))
        r2, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="bdg-2", task_type="slow", payload={},
            request_id="req-bdg2"))
        for _ in range(5):
            await asyncio.sleep(0)
        assert components.store.running_count() == 2
        assert (await svc.get(r2.id)).state is TaskState.PENDING

        r_budget = await svc.wait_until_terminal(r_budget.id, timeout=5)
        assert r_budget.error_code == "budget_exhausted"
        for _ in range(5):
            await asyncio.sleep(0)
        assert (await svc.get(r2.id)).state is TaskState.RUNNING
        assert components.store.running_count() == 2  # 没漏还也没超卖
        logger.info(
            "名额归还 req=req-bdg1 running=%s queue=%s -> 排队任务已补位",
            components.store.running_count(), components.store.queue_size(),
        )
    finally:
        h0.set(); h2.set()
        await components.executor.wait_until_terminal(r0.id, timeout=3)
        if r_budget is not None and r_budget.state is not TaskState.FAILED:
            await components.executor.wait_until_terminal(r_budget.id, timeout=3)
        rid2 = components.store.get_by_key("bdg-2")
        if rid2 is not None:
            await components.executor.wait_until_terminal(rid2.id, timeout=3)
    assert components.store.running_count() == 0


@pytest.mark.asyncio
async def test_default_behavior_without_overrides_unchanged(components):
    """不传覆盖参数：仍走默认重试逻辑（1 + 默认 max_retries=2 次尝试）。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="bd-4", task_type="flaky",
        payload={"fail_times": 99}, request_id="req-bd4",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 3
    assert rec.max_retries == 2
    assert rec.time_budget_seconds is None
    assert rec.deadline_monotonic is None
    assert rec.error_code == "transient_failure"
    logger.info(
        "默认行为 req=req-bd4 attempts=%s code=%s -> 老调用方逻辑不变",
        rec.attempts, rec.error_code,
    )


@pytest.mark.asyncio
async def test_budget_task_replay_reused_and_content_conflict_rejected(components):
    """预算任务终结后：同键同内容重放复用不重跑；同键异内容仍拒绝。"""
    svc = components.service
    kwargs = dict(
        idempotency_key="bd-5", task_type="sleep_ok",
        payload={"sleep": 0.35}, time_budget_seconds=0.15,
    )
    rec1, created1, _ = await svc.submit(
        SubmitRequest(request_id="req-bd5a", max_retries=2, **kwargs))
    rec1 = await svc.wait_until_terminal(rec1.id, timeout=5)
    assert rec1.error_code == "budget_exhausted"
    assert created1 is True

    rec2, created2, reason2 = await svc.submit(
        SubmitRequest(request_id="req-bd5b", max_retries=2, **kwargs))
    assert created2 is False and reason2 == "duplicate_replay"
    assert rec2.id == rec1.id
    assert rec2.attempts == 1  # 重放不重新执行

    with pytest.raises(IdempotencyConflictError) as excinfo:
        await svc.submit(
            SubmitRequest(request_id="req-bd5c", max_retries=3, **kwargs))
    assert excinfo.value.code == "idempotency_content_conflict"
    logger.info(
        "幂等不回归 key=bd-5 replay=%s conflict=%s attempts=%s -> 复用/拒绝规则保持",
        created2, excinfo.value.code, rec2.attempts,
    )
