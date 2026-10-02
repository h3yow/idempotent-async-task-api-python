"""按幂等键反查任务测试：纯读取、结果可区分、不触发执行。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.errors import TaskNotFoundError
from app.models import SubmitRequest, TaskState

from .handlers_helpers import make_holder

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_lookup_by_key_returns_current_state(components):
    """反查返回状态/尝试次数/失败原因/副作用标记/是否还会重试。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="lk-1", task_type="sensitive",
        payload={"fail_after_effect": True}, request_id="req-lk1",
    ))
    await svc.wait_until_terminal(rec.id, timeout=5)

    found = await svc.get_by_idempotency_key("lk-1")
    assert found.id == rec.id
    assert found.state is TaskState.FAILED
    assert found.attempts == 1
    assert found.error_code == "side_effect_committed"
    assert found.side_effect_occurred is True
    assert found.will_retry() is False
    logger.info(
        "按键反查 key=lk-1 task=%s state=%s attempts=%s side_effect=%s -> 命中",
        found.id, found.state.value, found.attempts, found.side_effect_occurred,
    )


@pytest.mark.asyncio
async def test_lookup_by_key_not_found_distinct(components):
    """查无此任务：抛出可区分的 TaskNotFoundError，不是空结果也不是 500。"""
    svc = components.service
    with pytest.raises(TaskNotFoundError) as excinfo:
        await svc.get_by_idempotency_key("lk-absent")
    assert excinfo.value.code == "task_not_found"
    logger.info("查无此键 key=lk-absent code=%s -> 明确可区分", excinfo.value.code)


@pytest.mark.asyncio
async def test_lookup_does_not_trigger_execution(components):
    """反查是纯读取：排队任务被反查后仍保持 PENDING，无任何副作用。"""
    svc = components.service
    h0, h1, h2 = (make_holder(f"lkq-{i}") for i in range(3))
    try:
        await svc.submit(SubmitRequest(
            idempotency_key="lkq-0", task_type="slow", payload={},
            request_id="req-lkq0"))
        await svc.submit(SubmitRequest(
            idempotency_key="lkq-1", task_type="slow", payload={},
            request_id="req-lkq1"))
        queued, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="lkq-2", task_type="slow", payload={},
            request_id="req-lkq2"))
        for _ in range(5):
            await asyncio.sleep(0)
        assert (await svc.get(queued.id)).state is TaskState.PENDING

        # 反复反查：状态不变、无执行、无副作用
        for _ in range(3):
            found = await svc.get_by_idempotency_key("lkq-2")
            assert found.state is TaskState.PENDING
            assert found.attempts == 0
        assert components.effects.count(queued.id) == 0
        assert components.store.running_count() == 2
        logger.info(
            "纯读取 key=lkq-2 state=PENDING attempts=0 running=%s -> 未触发执行",
            components.store.running_count(),
        )
    finally:
        h0.set(); h1.set(); h2.set()


@pytest.mark.asyncio
async def test_http_lookup_by_key(httpx_client):
    """HTTP 反查：命中返回完整视图；未命中返回 404 task_not_found。"""
    resp = await httpx_client.post("/tasks", json={
        "idempotency_key": "lk-http", "task_type": "flaky",
        "payload": {"permanent": True},
    }, headers={"x-request-id": "req-lkh1"})
    assert resp.status_code == 202
    task_id = resp.json()["task"]["task_id"]
    await asyncio.sleep(0.1)  # 等待执行终结

    got = await httpx_client.get("/tasks/by-key/lk-http",
                                 headers={"x-request-id": "req-lkh2"})
    assert got.status_code == 200
    body = got.json()
    assert body["task_id"] == task_id
    assert body["state"] == "FAILED"
    assert body["attempts"] == 1
    assert body["error_code"] == "permanent_failure"
    assert body["side_effect_occurred"] is False
    assert body["will_retry"] is False

    missing = await httpx_client.get("/tasks/by-key/lk-none",
                                     headers={"x-request-id": "req-lkh3"})
    assert missing.status_code == 404
    assert missing.json()["error_code"] == "task_not_found"
    logger.info(
        "HTTP 反查 req=req-lkh2 命中 task=%s；req=req-lkh3 -> 404 可区分",
        task_id,
    )


@pytest.mark.asyncio
async def test_lookup_after_replay_and_conflict_rule_intact(components):
    """反查不破坏幂等规则：同键同内容仍复用，同键异内容仍拒绝。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="lk-2", task_type="side_effect",
        payload={"value": "x"}, request_id="req-lk2a",
    ))
    await svc.wait_until_terminal(rec.id, timeout=5)

    # 反查之后再重放：仍复用同一任务，不重复执行
    await svc.get_by_idempotency_key("lk-2")
    rec2, created, _ = await svc.submit(SubmitRequest(
        idempotency_key="lk-2", task_type="side_effect",
        payload={"value": "x"}, request_id="req-lk2b",
    ))
    assert created is False and rec2.id == rec.id
    assert components.effects.count(rec.id) == 1

    # 同键异内容仍被拒绝
    from app.errors import IdempotencyConflictError

    with pytest.raises(IdempotencyConflictError):
        await svc.submit(SubmitRequest(
            idempotency_key="lk-2", task_type="side_effect",
            payload={"value": "y"}, request_id="req-lk2c",
        ))
    logger.info("规则保持 key=lk-2 重放复用 + 异内容拒绝 -> 反查无副作用")
