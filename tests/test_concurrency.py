"""并发上限与背压场景测试。"""

from __future__ import annotations

import logging

import pytest

from app.errors import QueueFullError
from app.models import SubmitRequest, TaskState

from .handlers_helpers import make_holder

logger = logging.getLogger("task_tests")


async def _submit_slow(svc, key: str, request_id: str):
    rec, created, reason = await svc.submit(SubmitRequest(
        idempotency_key=key, task_type="slow",
        payload={"max_wait": 10}, request_id=request_id,
    ))
    return rec, created, reason


@pytest.mark.asyncio
async def test_concurrency_cap_not_oversold(components):
    """运行任务数不得超过 max_concurrency=2，超出部分排队。"""
    svc = components.service
    h1, h2, h3, h4 = (make_holder(f"cap-{i}") for i in range(4))
    try:
        r1, _, why1 = await _submit_slow(svc, "cap-0", "req-cap1")
        r2, _, why2 = await _submit_slow(svc, "cap-1", "req-cap2")
        r3, _, why3 = await _submit_slow(svc, "cap-2", "req-cap3")
        r4, _, why4 = await _submit_slow(svc, "cap-3", "req-cap4")
        await asyncio_sleep_tick()

        assert why1 == "created_running" and why2 == "created_running"
        assert why3 == "created_queued" and why4 == "created_queued"
        assert components.store.running_count() == 2
        assert components.store.queue_size() == 2
        assert (await svc.get(r3.id)).state is TaskState.PENDING
        logger.info(
            "容量观测 req=req-cap* running=%s/2 queue=%s/2 -> 未超卖",
            components.store.running_count(), components.store.queue_size(),
        )
    finally:
        for h in (h1, h2, h3, h4):
            h.set()
        for rid in (r1.id, r2.id, r3.id, r4.id):
            await components.executor.wait_until_terminal(rid, timeout=3)


@pytest.mark.asyncio
async def test_queue_fifo_order(components):
    """容量不足时按 FIFO 确定顺序进入执行。"""
    svc = components.service
    keys = ["fifo-0", "fifo-1", "fifo-2", "fifo-3"]
    holders = [make_holder(k) for k in keys]
    records = []
    try:
        for i, k in enumerate(keys):
            rec, _, _ = await _submit_slow(svc, k, f"req-fifo{i}")
            records.append(rec)
        await asyncio_sleep_tick()
        # 前两个占槽，后两个排队；队首是 fifo-2
        assert components.store.queued_position(records[2].id) == 0
        assert components.store.queued_position(records[3].id) == 1

        # 按释放顺序观察：放一个、完成一个，队首补位
        holders[0].set()
        await components.executor.wait_until_terminal(records[0].id, timeout=3)
        await asyncio_sleep_tick()
        assert (await svc.get(records[2].id)).state is TaskState.RUNNING
        assert (await svc.get(records[3].id)).state is TaskState.PENDING
        logger.info("FIFO req=req-fifo* fifo-2 在 fifo-3 之前补位 -> 顺序确定")
    finally:
        for h in holders:
            h.set()
        for rec in records:
            await components.executor.wait_until_terminal(rec.id, timeout=3)


@pytest.mark.asyncio
async def test_queue_full_rejected_with_distinct_reason(components):
    """并发槽位与队列均满（2+2）：第 5 个请求以可区分原因拒绝。"""
    svc = components.service
    keys = [f"full-{i}" for i in range(5)]
    holders = [make_holder(k) for k in keys]
    recs = []
    try:
        for i, k in enumerate(keys[:4]):
            rec, _, _ = await _submit_slow(svc, k, f"req-full{i}")
            recs.append(rec)
        with pytest.raises(QueueFullError) as excinfo:
            await _submit_slow(svc, keys[4], "req-full4")
        assert excinfo.value.code == "capacity_queue_full"
        # 被拒绝后容量未变化，且该幂等键未被占用（容量恢复后可用同键重试）
        assert components.store.running_count() == 2
        assert components.store.queue_size() == 2
        assert components.store.get_by_key(keys[4]) is None
        logger.info(
            "过载拒绝 req=req-full4 code=%s running=%s queue=%s -> 未静默丢弃",
            excinfo.value.code,
            components.store.running_count(),
            components.store.queue_size(),
        )
    finally:
        for h in holders[:4]:
            h.set()
        for rec in recs:
            await components.executor.wait_until_terminal(rec.id, timeout=3)


@pytest.mark.asyncio
async def test_finished_task_releases_slot(components):
    """任务结束释放槽位，排队任务自动进入执行。"""
    svc = components.service
    h0, h1, h2 = make_holder("rel-0"), make_holder("rel-1"), make_holder("rel-2")
    r0, _, _ = await _submit_slow(svc, "rel-0", "req-rel0")
    r1, _, _ = await _submit_slow(svc, "rel-1", "req-rel1")
    r2, _, _ = await _submit_slow(svc, "rel-2", "req-rel2")
    await asyncio_sleep_tick()
    assert (await svc.get(r2.id)).state is TaskState.PENDING
    h0.set()
    await components.executor.wait_until_terminal(r0.id, timeout=3)
    await asyncio_sleep_tick()
    assert (await svc.get(r2.id)).state is TaskState.RUNNING
    assert components.store.running_count() == 2
    h1.set(); h2.set()
    await components.executor.wait_until_terminal(r1.id, timeout=3)
    await components.executor.wait_until_terminal(r2.id, timeout=3)


async def asyncio_sleep_tick() -> None:
    """让出事件循环若干拍，等待调度器反应。"""
    import asyncio

    for _ in range(5):
        await asyncio.sleep(0)
