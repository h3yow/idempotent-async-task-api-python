"""失败分类、重试、取消与状态流转测试。"""

from __future__ import annotations

import logging

import pytest

from app.models import SubmitRequest, TaskState

from .handlers_helpers import make_holder

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_transient_failure_retried_then_succeeds(components):
    """瞬时失败重试后成功：状态流转一致，副作用只来自真实尝试。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="retry-ok", task_type="side_effect",
        payload={"fail_times": 2}, request_id="req-retry1",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.attempts == 3
    assert rec.failure_kind == "transient"
    states = [(s.value, d.value) for _, _, s, d, _ in rec.history]
    assert ("RUNNING", "PENDING") in states  # 瞬时失败后重新排队
    assert states[-1] == ("RUNNING", "SUCCEEDED")
    # 每次尝试只执行一次副作用，共 3 次真实调用（失败尝试也执行了副作用逻辑前半段）
    assert components.effects.attempts(rec.id) == [1, 2, 3]
    logger.info(
        "瞬时重试 req=req-retry1 task=%s attempts=%s 流转=%s -> 重试后成功",
        rec.id, rec.attempts, states,
    )


@pytest.mark.asyncio
async def test_permanent_failure_no_retry(components):
    """永久失败：不重试直接 FAILED，failure_kind 可区分。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="perm-1", task_type="side_effect",
        payload={"permanent": True}, request_id="req-perm1",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 1
    assert rec.failure_kind == "permanent"
    assert rec.error_code == "permanent_failure"
    assert components.effects.count(rec.id) == 0  # 副作用记录点在抛错之后
    logger.info(
        "永久失败 req=req-perm1 task=%s code=%s attempts=%s -> 不重试",
        rec.id, rec.error_code, rec.attempts,
    )


@pytest.mark.asyncio
async def test_retries_exhausted_terminal_failed(components):
    """瞬时失败超过最大重试次数（max_retries=2，共 3 次尝试）：终态 FAILED。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="retry-exhaust", task_type="flaky",
        payload={"fail_times": 99}, request_id="req-exh1",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 3
    assert rec.failure_kind == "transient"
    assert rec.error_code == "transient_failure"
    last_note = rec.history[-1][4]
    assert "retries exhausted" in last_note
    logger.info(
        "重试耗尽 req=req-exh1 task=%s attempts=%s note=%r -> 终态可解释",
        rec.id, rec.attempts, last_note,
    )


@pytest.mark.asyncio
async def test_cancel_queued_task(components):
    """取消排队任务：直接 CANCELLED，后续不执行、无副作用。"""
    svc = components.service
    h0, h1, h2 = make_holder("cx-0"), make_holder("cx-1"), make_holder("cx-2")
    try:
        await svc.submit(SubmitRequest(
            idempotency_key="cx-0", task_type="slow", payload={},
            request_id="req-cx0"))
        await svc.submit(SubmitRequest(
            idempotency_key="cx-1", task_type="slow", payload={},
            request_id="req-cx1"))
        queued, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="cx-2", task_type="slow", payload={},
            request_id="req-cx2"))
        import asyncio
        for _ in range(3):
            await asyncio.sleep(0)
        assert (await svc.get(queued.id)).state is TaskState.PENDING
        cancelled = await svc.cancel(queued.id)
        assert cancelled.state is TaskState.CANCELLED
        assert components.effects.count(queued.id) == 0
        # 取消是幂等的：再次取消仍返回 CANCELLED，且不报错
        again = await svc.cancel(queued.id)
        assert again.state is TaskState.CANCELLED
        logger.info(
            "取消排队 req=req-cx2 task=%s -> CANCELLED，无副作用，取消可重入",
            queued.id,
        )
    finally:
        h0.set(); h1.set(); h2.set()


@pytest.mark.asyncio
async def test_state_history_traceable(components):
    """执行链路可追踪：history 记录时间戳、请求标识、流转说明。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="trace-1", task_type="side_effect",
        payload={"fail_times": 1}, request_id="req-trace1",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED
    transitions = [(src.value, dst.value, note)
                   for _, rid, src, dst, note in rec.history]
    assert all(rid == "req-trace1" for _, rid, _, _, _ in rec.history)
    assert ("PENDING", "RUNNING", "attempt 1 start") in transitions
    assert any(src == "RUNNING" and dst == "PENDING" for src, dst, _ in transitions)
    assert transitions[-1][:2] == ("RUNNING", "SUCCEEDED")
    for at, rid, src, dst, note in rec.history:
        logger.info(
            "链路 req=%s %.4f %s->%s note=%r", rid, at, src.value, dst.value, note,
        )


@pytest.mark.asyncio
async def test_cancel_during_retry_backoff(components):
    """瞬时失败进入退避/重新排队后取消：任务终结为 CANCELLED，不再继续重试。"""
    # 放大退避，使取消稳定落在退避窗口内
    components.executor._settings = type(components.executor._settings)(
        max_concurrency=2,
        max_queue_size=2,
        task_timeout_seconds=1.0,
        max_retries=2,
        retry_backoff_seconds=0.5,
    )
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="backoff-cancel", task_type="flaky",
        payload={"fail_times": 99}, request_id="req-bc1",
    ))
    import asyncio
    await asyncio.sleep(0.05)  # 等到第一次尝试失败、进入退避
    cancelled = await svc.cancel(rec.id)
    assert cancelled.state is TaskState.CANCELLED
    final = await svc.wait_until_terminal(rec.id, timeout=3)
    assert final.state is TaskState.CANCELLED
    assert final.attempts == 1  # 取消后不再发起后续尝试
    logger.info(
        "退避取消 req=req-bc1 attempts=%s 末态=%s -> 重试与取消流转一致",
        final.attempts, final.state.value,
    )
