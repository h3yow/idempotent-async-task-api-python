"""整体时间预算测试：预算是真正的整体上界，到点立即打断执行。

与单次执行超时（timeout，按瞬时失败可重试）清晰区分：
预算用尽一律以 budget_exhausted 终结，不重试。
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
async def test_budget_cut_mid_execution_even_would_succeed(components):
    """预算 < 本次执行耗时 < 单次超时：预算到点立即打断，以预算用尽终结。

    即使这次执行眼看就要成功（0.8s 后就会返回），也绝不允许跑过预算。
    """
    svc = components.service
    started = time.monotonic()
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="bud-1", task_type="sleepy",
        payload={"sleep": 0.8},  # 小于单次超时 1.0s，大于预算 0.1s
        request_id="req-bud1", time_budget_seconds=0.1,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    elapsed = time.monotonic() - started

    assert rec.state is TaskState.FAILED
    assert rec.error_code == "budget_exhausted"
    assert rec.failure_kind == "budget_exhausted"
    # 与单次执行超时清晰区分
    assert rec.error_code != "timeout"
    assert rec.attempts == 1                       # 预算用尽不重试
    assert rec.will_retry() is False
    # 在预算到点（0.1s）附近就被打断，远早于单次超时（1.0s）
    assert elapsed < 0.8
    # 执行被中断，成功路径上的副作用没有发生
    assert components.effects.count(rec.id) == 0
    logger.info(
        "预算硬上界 req=req-bud1 task=%s elapsed=%.3fs attempts=%s effects=%s "
        "code=%s -> 到点即停",
        rec.id, elapsed, rec.attempts, components.effects.count(rec.id),
        rec.error_code,
    )


@pytest.mark.asyncio
async def test_budget_exhausted_releases_slot_and_queue_progresses(components):
    """预算用尽提前终结后：并发名额正常归还，排队任务继续跑，不漏还。"""
    svc = components.service
    h0 = make_holder("budq-0")
    h2 = make_holder("budq-2")
    try:
        # 占满两个槽位
        r0, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="budq-0", task_type="slow", payload={},
            request_id="req-budq0"))
        r1, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="budq-1", task_type="sleepy",
            payload={"sleep": 0.8}, request_id="req-budq1",
            time_budget_seconds=0.1))
        # 排队任务
        r2, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="budq-2", task_type="slow", payload={},
            request_id="req-budq2"))
        for _ in range(5):
            await asyncio.sleep(0)
        assert components.store.running_count() == 2
        assert (await svc.get(r2.id)).state is TaskState.PENDING

        # 预算任务到点终结：名额归还，排队任务补位
        r1 = await svc.wait_until_terminal(r1.id, timeout=5)
        assert r1.error_code == "budget_exhausted"
        for _ in range(5):
            await asyncio.sleep(0)
        assert (await svc.get(r2.id)).state is TaskState.RUNNING
        assert components.store.running_count() == 2  # 没漏还也没超卖
        logger.info(
            "名额归还 req=req-budq1 running=%s queue=%s -> 排队任务已补位",
            components.store.running_count(), components.store.queue_size(),
        )
    finally:
        h0.set(); h2.set()
        await components.executor.wait_until_terminal(r0.id, timeout=3)
        await components.executor.wait_until_terminal(r2.id, timeout=3)
    # 全部结束后名额完全释放
    assert components.store.running_count() == 0


@pytest.mark.asyncio
async def test_default_no_budget_unchanged(components):
    """不传 time_budget_seconds：无预算截止，走原来的默认逻辑（老调用方不变）。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="bud-2", task_type="sleepy",
        payload={"sleep": 0.2},  # 无预算时只要不超过单次超时即可成功
        request_id="req-bud2",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.time_budget_seconds is None
    assert rec.deadline_monotonic is None
    assert components.effects.count(rec.id) == 1
    logger.info(
        "默认行为 req=req-bud2 task=%s budget=%s -> 无预算照常成功",
        rec.id, rec.time_budget_seconds,
    )


@pytest.mark.asyncio
async def test_budget_task_replay_and_conflict(components):
    """带预算的任务：同键同内容重放复用记录不重复执行；同键异内容拒绝。"""
    svc = components.service
    rec, created, _ = await svc.submit(SubmitRequest(
        idempotency_key="bud-3", task_type="sleepy",
        payload={"sleep": 0.05}, request_id="req-bud3a",
        time_budget_seconds=5,
    ))
    assert created is True
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED

    # 同键同内容（含相同预算）：复用同一任务，不重复执行
    rec2, created2, reason2 = await svc.submit(SubmitRequest(
        idempotency_key="bud-3", task_type="sleepy",
        payload={"sleep": 0.05}, request_id="req-bud3b",
        time_budget_seconds=5,
    ))
    assert created2 is False and reason2 == "duplicate_replay"
    assert rec2.id == rec.id
    assert rec2.attempts == 1
    assert components.effects.count(rec.id) == 1

    # 同键不同内容（预算取值不同也算不同内容）：明确拒绝
    with pytest.raises(IdempotencyConflictError):
        await svc.submit(SubmitRequest(
            idempotency_key="bud-3", task_type="sleepy",
            payload={"sleep": 0.05}, request_id="req-bud3c",
            time_budget_seconds=10,
        ))
    logger.info(
        "幂等重放 req=req-bud3b task=%s effects=%s；异内容 req=req-bud3c -> 拒绝",
        rec2.id, components.effects.count(rec.id),
    )


@pytest.mark.asyncio
async def test_single_timeout_without_budget_still_retriable(components):
    """对照组：不设预算时单次超时仍按瞬时失败处理（可重试），原因保持 timeout。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="bud-4", task_type="sleepy",
        payload={"sleep": 5.0},  # 超过单次超时 1.0s，但未设整体预算
        request_id="req-bud4", max_retries=1,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=10)
    assert rec.state is TaskState.FAILED
    assert rec.error_code == "timeout"
    assert rec.failure_kind == "transient"
    assert rec.attempts == 2                       # 超时可重试，与预算用尽不同
    logger.info(
        "对照组 req=req-bud4 task=%s attempts=%s code=%s -> 单次超时仍可重试",
        rec.id, rec.attempts, rec.error_code,
    )
