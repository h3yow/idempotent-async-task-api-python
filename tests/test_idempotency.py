"""幂等重复提交与同键异内容冲突测试。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from task_api.errors import ErrorCode, IdempotencyConflict
from task_api.models import SubmitRequest, TaskStatus
from tests.conftest import Harness, make_settings  # noqa: F401  (类型/复用)

logger = logging.getLogger("task_api.test.idempotency")


@pytest.mark.asyncio
async def test_duplicate_submit_returns_same_task_and_runs_once(harness: Harness):
    body = SubmitRequest(
        task_type="echo", payload={"value": 1}, idempotency_key="key-1"
    )
    v1 = await harness.submit("echo", "key-1", {"value": 1})
    await harness.wait(v1.task_id)

    # 同键同内容重复提交：必须返回同一任务
    v2 = await harness.service.submit(body, request_id="req-dup", trace_id="trace-dup")
    assert v2.task_id == v1.task_id
    assert v2.status == TaskStatus.SUCCEEDED

    records = await harness.store.all()
    assert len(records) == 1, "重复提交产生了重复任务"
    # 处理器只运行一次：没有重复副作用
    echo_calls = [c for c in harness.calls if c.startswith("echo:")]
    assert len(echo_calls) == 1
    logger.info(
        "duplicate submit deduped task=%s handler_calls=%s",
        v1.task_id, echo_calls,
    )


@pytest.mark.asyncio
async def test_same_key_different_payload_is_rejected_with_distinct_code(harness: Harness):
    v = await harness.submit("echo", "key-conflict", {"value": 1})
    await harness.wait(v.task_id)

    # 仅 payload 不同
    with pytest.raises(IdempotencyConflict) as payload_conflict:
        await harness.submit("echo", "key-conflict", {"value": 2})
    assert payload_conflict.value.error_code == ErrorCode.IDEMPOTENCY_KEY_CONFLICT
    assert v.task_id in payload_conflict.value.message

    # 仅 task_type 不同（payload 相同）也必须冲突
    async def _make_other_type_handler():
        async def _handler(payload, ctx):
            return None
        return _handler

    harness.registry.register("other", await _make_other_type_handler())
    with pytest.raises(IdempotencyConflict) as type_conflict:
        await harness.submit("other", "key-conflict", {"value": 1})
    assert type_conflict.value.error_code == ErrorCode.IDEMPOTENCY_KEY_CONFLICT

    # 冲突不影响既有任务，也没有新建任务
    records = await harness.store.all()
    assert len(records) == 1
    logger.info("conflict correctly rejected: %s", payload_conflict.value.message)


@pytest.mark.asyncio
async def test_concurrent_duplicate_submits_create_single_task():
    from tests.conftest import Harness

    h = Harness(make_settings())
    try:
        # 多个协程同时用相同键与内容提交，只允许创建一个任务
        results = await asyncio.gather(
            *[h.submit("echo", "race-key", {"v": 1}) for _ in range(8)]
        )
        task_ids = {r.task_id for r in results}
        assert len(task_ids) == 1
        await h.wait(next(iter(task_ids)))
        records = await h.store.all()
        assert len(records) == 1
    finally:
        await h.shutdown()


async def _noop():
    return None
