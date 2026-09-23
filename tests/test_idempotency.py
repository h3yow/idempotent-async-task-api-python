"""幂等提交场景测试。"""

from __future__ import annotations

import logging

import pytest

from app.errors import IdempotencyConflictError
from app.models import SubmitRequest, TaskState

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_duplicate_submission_returns_same_task(components):
    """相同幂等键重复提交：复用同一任务，不产生重复任务/副作用。"""
    svc = components.service
    req1 = SubmitRequest(
        idempotency_key="idem-1", task_type="side_effect",
        payload={"value": "a"}, request_id="req-A",
    )
    rec1, created1, reason1 = await svc.submit(req1)
    assert created1 is True and reason1 == "created_running"
    rec1 = await svc.wait_until_terminal(rec1.id)
    assert rec1.state is TaskState.SUCCEEDED

    # 不同入口（新 request_id）携带同一幂等键重放
    req2 = SubmitRequest(
        idempotency_key="idem-1", task_type="side_effect",
        payload={"value": "a"}, request_id="req-B",
    )
    rec2, created2, reason2 = await svc.submit(req2)
    assert created2 is False and reason2 == "duplicate_replay"
    assert rec2.id == rec1.id
    assert rec2.attempts == 1
    assert components.effects.count(rec1.id) == 1
    logger.info(
        "幂等重放 req=req-B task=%s state=%s attempts=%s effects=%s -> 无重复副作用",
        rec2.id, rec2.state.value, rec2.attempts,
        components.effects.count(rec1.id),
    )


@pytest.mark.asyncio
async def test_same_key_different_content_rejected(components):
    """相同幂等键 + 不同内容：明确拒绝并给出可区分原因。"""
    svc = components.service
    await svc.submit(SubmitRequest(
        idempotency_key="idem-2", task_type="side_effect",
        payload={"v": 1}, request_id="req-C",
    ))
    with pytest.raises(IdempotencyConflictError) as excinfo:
        await svc.submit(SubmitRequest(
            idempotency_key="idem-2", task_type="side_effect",
            payload={"v": 2}, request_id="req-D",
        ))
    assert excinfo.value.code == "idempotency_content_conflict"
    # 不同 task_type 同样属于不同内容
    with pytest.raises(IdempotencyConflictError):
        await svc.submit(SubmitRequest(
            idempotency_key="idem-2", task_type="flaky",
            payload={"v": 1}, request_id="req-E",
        ))
    logger.info(
        "幂等冲突 req=req-D code=%s -> 拒绝原因可区分", excinfo.value.code)


@pytest.mark.asyncio
async def test_duplicate_after_completion_no_redo(components):
    """任务终态后重复提交：返回既有结果，不再执行，无额外副作用。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="idem-3", task_type="side_effect",
        payload={"value": "done"}, request_id="req-F",
    ))
    rec = await svc.wait_until_terminal(rec.id)
    assert rec.result == {"attempt": 1, "echo": "done"}
    n_before = components.effects.count(rec.id)

    rec2, created, _ = await svc.submit(SubmitRequest(
        idempotency_key="idem-3", task_type="side_effect",
        payload={"value": "done"}, request_id="req-G",
    ))
    assert created is False
    assert rec2.state is TaskState.SUCCEEDED
    assert rec2.result == rec.result
    assert components.effects.count(rec.id) == n_before
