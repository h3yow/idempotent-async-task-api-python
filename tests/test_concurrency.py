"""并发上限、FIFO 排队顺序与背压拒绝测试。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from task_api.errors import ErrorCode, QueueFull, TaskNotCancellable
from task_api.models import TaskStatus

logger = logging.getLogger("task_api.test.concurrency")


@pytest.mark.asyncio
async def test_running_never_exceeds_max_concurrency(harness):
    settings = harness.settings
    # 并发上限 2，提交远超 (2 并发 + 4 队列) 的任务会被拒绝，这里只提 6 个可容纳的
    total = settings.max_concurrency + settings.queue_capacity  # 6
    views = []
    for i in range(total):
        views.append(
            await harness.submit(
                "blocking", f"conc-{i}", {"gate": "g"}
            )
        )

    await asyncio.sleep(0.05)  # 等调度稳定
    assert harness.executor.stats.max_running_seen <= settings.max_concurrency
    assert harness.executor.stats.running == settings.max_concurrency
    assert harness.executor.stats.waiting == settings.queue_capacity
    logger.info(
        "peak running=%d/%d waiting=%d/%d",
        harness.executor.stats.max_running_seen,
        settings.max_concurrency,
        harness.executor.stats.waiting,
        settings.queue_capacity,
    )

    harness.gate("g").set()
    for view in views:
        rec = await harness.wait(view.task_id)
        assert rec.status == TaskStatus.SUCCEEDED
    # 全程观测到的最大运行数恰好等于上限（额度被用满但未超卖）
    assert harness.executor.stats.max_running_seen == settings.max_concurrency


@pytest.mark.asyncio
async def test_queue_full_is_rejected_with_distinct_code_and_terminal_record(harness):
    capacity_total = (
        harness.settings.max_concurrency + harness.settings.queue_capacity
    )
    for i in range(capacity_total):
        await harness.submit("blocking", f"fill-{i}", {"gate": "full"})
    await asyncio.sleep(0.05)

    # 再提交：并发满 + 队列满 -> QUEUE_FULL
    with pytest.raises(QueueFull) as rejected:
        await harness.submit("blocking", "overflow", {"gate": "full"})
    assert rejected.value.error_code == ErrorCode.QUEUE_FULL

    # 被拒任务必须以终态留痕（不静默丢弃），且原因可区分
    rec = await harness.store.get_by_idempotency_key("overflow")
    assert rec is not None
    assert rec.status == TaskStatus.CANCELLED
    assert rec.error_code == ErrorCode.QUEUE_FULL
    logger.info("overflow rejected, terminal=%s", rec.status.value)


@pytest.mark.asyncio
async def test_fifo_queue_order_is_deterministic():
    from tests.conftest import Harness, make_settings

    # 单并发：第二个起的任务必然排队，完成顺序应严格等于提交顺序
    h = Harness(make_settings(max_concurrency=1, queue_capacity=10))
    try:
        order: list[str] = []

        async def ordered(payload, ctx):
            order.append(payload["name"])
            await asyncio.sleep(0.005)
            return payload["name"]

        h.registry.register("ordered", ordered)
        names = [f"n{i}" for i in range(6)]
        views = []
        for name in names:
            views.append(await h.submit("ordered", f"key-{name}", {"name": name}))
        for view in views:
            await h.wait(view.task_id)
        assert order == names, f"FIFO 顺序被破坏: {order}"
        logger.info("FIFO completion order=%s", order)
    finally:
        await h.shutdown()


@pytest.mark.asyncio
async def test_cancel_queued_task_frees_capacity_and_terminal_is_not_cancellable(
    harness,
):
    # 占满并发与队列
    accepted = []
    for i in range(
        harness.settings.max_concurrency + harness.settings.queue_capacity
    ):
        accepted.append(
            await harness.submit("blocking", f"c-{i}", {"gate": "cg"})
        )
    await asyncio.sleep(0.05)

    # accepted[max_concurrency:] 是等待队列；取消队首等待任务
    queued = accepted[harness.settings.max_concurrency]
    resp = await harness.service.cancel(queued.task_id)
    assert resp.cancelled is True
    assert resp.status == TaskStatus.CANCELLED

    # 取消后队列腾出一个位置，新的提交应被接受（不再 QUEUE_FULL）
    freed = await harness.submit("blocking", "freed", {"gate": "cg"})
    assert freed.status == TaskStatus.QUEUED

    # 已终态任务再次取消必须被拒绝，原因可区分
    with pytest.raises(TaskNotCancellable) as not_cancel:
        await harness.service.cancel(queued.task_id)
    assert not_cancel.value.error_code in {
        ErrorCode.TASK_NOT_CANCELLABLE,
        ErrorCode.TASK_ALREADY_TERMINAL,
    }
    logger.info("queued cancel ok; re-cancel rejected: %s", not_cancel.value.message)
