"""提交级覆盖参数：max_retries 与 time_budget_seconds 的生效、校验与兼容性。"""

from __future__ import annotations

import logging

import pytest

from app.models import SubmitRequest, TaskState

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_override_max_retries_zero_fails_fast(components):
    """max_retries=0：瞬时失败不再重试，首次失败即终结（尽快失败诉求）。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-r0", task_type="flaky",
        payload={"fail_times": 99}, request_id="req-ovr0",
        max_retries=0,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 1
    assert rec.max_retries == 0
    assert rec.failure_kind == "transient"
    logger.info(
        "覆盖重试 req=req-ovr0 task=%s max_retries=0 attempts=%s -> 立即失败不重试",
        rec.id, rec.attempts,
    )


@pytest.mark.asyncio
async def test_override_max_retries_above_default(components):
    """max_retries=4（默认 2）：多试几次后成功，覆盖值在记录中可见。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-r4", task_type="flaky",
        payload={"fail_times": 3}, request_id="req-ovr4",
        max_retries=4,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.attempts == 4
    assert rec.max_retries == 4
    logger.info(
        "覆盖重试 req=req-ovr4 task=%s max_retries=4 attempts=%s -> 多试几次后成功",
        rec.id, rec.attempts,
    )


@pytest.mark.asyncio
async def test_default_behavior_unchanged_without_overrides(components):
    """不指定覆盖参数：完全沿用默认（max_retries=2，共 3 次尝试后耗尽）。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-default", task_type="flaky",
        payload={"fail_times": 99}, request_id="req-ovd",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 3
    assert rec.max_retries == 2
    assert rec.time_budget_seconds is None
    assert rec.deadline_at is None
    logger.info(
        "默认行为 req=req-ovd task=%s attempts=%s max_retries=%s -> 老调用方不受影响",
        rec.id, rec.attempts, rec.max_retries,
    )


@pytest.mark.asyncio
async def test_time_budget_exhausted_distinct_from_attempt_timeout(components):
    """整体预算用尽：error_code=budget_exhausted，与单次超时 timeout 可区分。"""
    svc = components.service
    # 单次超时阈值 1.0s，整体预算 0.2s：预算先耗尽
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-budget", task_type="timeout_then_ok",
        payload={"sleep": 0.5}, request_id="req-ovb",
        max_retries=5, time_budget_seconds=0.2,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.error_code == "budget_exhausted"
    assert rec.failure_kind == "budget_exhausted"
    assert rec.time_budget_seconds == 0.2
    assert rec.deadline_at is not None

    # 对照组：不设预算，单次超时按 timeout 归类并可重试
    rec2, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-no-budget", task_type="timeout_then_ok",
        payload={"sleep": 2.0}, request_id="req-ovt",
        max_retries=0,
    ))
    rec2 = await svc.wait_until_terminal(rec2.id, timeout=5)
    assert rec2.state is TaskState.FAILED
    assert rec2.error_code == "timeout"
    assert rec2.failure_kind == "transient"
    logger.info(
        "预算用尽 req=req-ovb task=%s code=%s kind=%s ｜ 对照 req=req-ovt code=%s -> 两者可区分",
        rec.id, rec.error_code, rec.failure_kind, rec2.error_code,
    )


@pytest.mark.asyncio
async def test_invalid_overrides_rejected_with_reason(httpx_client):
    """非法覆盖参数：400 invalid_parameter，且说明具体原因。"""
    cases = [
        ({"max_retries": -1}, "max_retries"),
        ({"max_retries": 11}, "max_retries"),
        ({"max_retries": 1.5}, "max_retries"),
        ({"time_budget_seconds": 0.01}, "time_budget_seconds"),
        ({"time_budget_seconds": 7200}, "time_budget_seconds"),
        ({"time_budget_seconds": -5}, "time_budget_seconds"),
    ]
    for i, (override, field) in enumerate(cases):
        resp = await httpx_client.post("/tasks", json={
            "idempotency_key": f"ov-bad-{i}", "task_type": "flaky",
            "payload": {}, **override},
            headers={"x-request-id": f"req-ovbad{i}"})
        assert resp.status_code == 400, (override, resp.status_code)
        body = resp.json()
        assert body["error_code"] == "invalid_parameter"
        assert field in body["message"]
        logger.info(
            "参数拒绝 req=req-ovbad%s %s=%s code=%s -> 400 原因明确",
            i, field, list(override.values())[0], body["error_code"],
        )


@pytest.mark.asyncio
async def test_duplicate_replay_with_same_overrides_reuses_task(httpx_client):
    """同键同内容（含相同覆盖参数）重放：复用同一任务，不重复执行。"""
    payload = {"idempotency_key": "ov-replay", "task_type": "side_effect",
               "payload": {"v": 1}, "max_retries": 3, "time_budget_seconds": 30}
    first = await httpx_client.post("/tasks", json=payload,
                                    headers={"x-request-id": "req-ovr1"})
    assert first.status_code == 202
    task_id = first.json()["task"]["task_id"]
    assert first.json()["task"]["max_retries"] == 3
    assert first.json()["task"]["time_budget_seconds"] == 30

    second = await httpx_client.post("/tasks", json=payload,
                                     headers={"x-request-id": "req-ovr2"})
    body = second.json()
    assert body["created"] is False
    assert body["reason"] == "duplicate_replay"
    assert body["task"]["task_id"] == task_id
    logger.info("同参重放 req=req-ovr2 task=%s -> 复用不重复执行", task_id)


@pytest.mark.asyncio
async def test_same_key_different_overrides_conflicts(httpx_client):
    """同键但覆盖参数不同：视为不同内容，409 拒绝（不静默忽略新参数）。"""
    await httpx_client.post("/tasks", json={
        "idempotency_key": "ov-conflict", "task_type": "flaky",
        "payload": {}, "max_retries": 1})
    resp = await httpx_client.post("/tasks", json={
        "idempotency_key": "ov-conflict", "task_type": "flaky",
        "payload": {}, "max_retries": 5})
    assert resp.status_code == 409
    assert resp.json()["error_code"] == "idempotency_content_conflict"
    logger.info("同键不同覆盖 key=ov-conflict -> 409 拒绝")
