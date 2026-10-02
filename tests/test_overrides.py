"""提交级覆盖参数测试：max_retries / time_budget_seconds。"""

from __future__ import annotations

import logging

import pytest

from app.errors import InvalidRequestParameterError
from app.models import SubmitRequest, TaskState

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_override_max_retries_zero_fails_fast(components):
    """max_retries=0：瞬时失败立即终结，只尝试一次（尽快失败诉求）。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-1", task_type="flaky",
        payload={"fail_times": 99}, request_id="req-ov1",
        max_retries=0,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 1
    assert rec.max_retries == 0
    assert rec.error_code == "transient_failure"
    logger.info(
        "覆盖重试 req=req-ov1 max_retries=0 attempts=%s -> 尽快失败",
        rec.attempts,
    )


@pytest.mark.asyncio
async def test_override_max_retries_higher_allows_more(components):
    """max_retries=5（默认 2 会耗尽）：多试几次后成功。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-2", task_type="flaky",
        payload={"fail_times": 3}, request_id="req-ov2",
        max_retries=5,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.attempts == 4
    assert rec.max_retries == 5
    logger.info(
        "覆盖重试 req=req-ov2 max_retries=5 attempts=%s -> 多试几次成功",
        rec.attempts,
    )


@pytest.mark.asyncio
async def test_default_behavior_unchanged_without_overrides(components):
    """不指定覆盖参数：完全沿用 Settings 默认（老调用方不受影响）。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-3", task_type="flaky",
        payload={"fail_times": 99}, request_id="req-ov3",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 3            # 1 + 默认 max_retries=2
    assert rec.max_retries == 2
    assert rec.time_budget_seconds is None
    assert rec.deadline_monotonic is None


@pytest.mark.asyncio
async def test_invalid_overrides_rejected(components):
    """超范围/非法覆盖值：明确拒绝并说明原因，且不占用幂等键。"""
    svc = components.service
    for bad in (-1, 11, True, 1.5, "3"):
        with pytest.raises(InvalidRequestParameterError) as excinfo:
            await svc.submit(SubmitRequest(
                idempotency_key=f"ov-bad-{bad}", task_type="flaky",
                payload={}, request_id="req-ovb",
                max_retries=bad,  # type: ignore[arg-type]
            ))
        assert excinfo.value.code == "invalid_request_parameter"
        assert "max_retries" in str(excinfo.value)
    for bad_budget in (0, -5, 3601, True, "10"):
        with pytest.raises(InvalidRequestParameterError) as excinfo:
            await svc.submit(SubmitRequest(
                idempotency_key=f"ov-badb-{bad_budget}", task_type="flaky",
                payload={}, request_id="req-ovb2",
                time_budget_seconds=bad_budget,  # type: ignore[arg-type]
            ))
        assert excinfo.value.code == "invalid_request_parameter"
        assert "time_budget_seconds" in str(excinfo.value)
    # 被拒请求不占用幂等键：修正参数后同键可正常提交
    rec, created, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-bad--1", task_type="flaky",
        payload={}, request_id="req-ovb3", max_retries=1,
    ))
    assert created is True
    logger.info("非法覆盖 req=req-ovb* -> 全部 422 语义拒绝且不占键")


@pytest.mark.asyncio
async def test_time_budget_exhausted_distinct_from_timeout():
    """整体预算用尽：以 budget_exhausted 终结，与单次超时 timeout 可区分。"""
    from app.config import Settings
    from app.handlers import HandlerRegistry
    from app.wiring import build_service

    from .handlers_helpers import EffectLog, install_test_handlers

    # 退避 0.3s、预算 0.05s：首次失败后预算在退避窗口内耗尽
    settings = Settings(
        max_concurrency=1, max_queue_size=4, task_timeout_seconds=1.0,
        max_retries=2, retry_backoff_seconds=0.3,
    )
    handlers = HandlerRegistry()
    install_test_handlers(handlers, EffectLog())
    service, executor, _ = build_service(
        settings, handlers=handlers,
        log=lambda m: logger.info("    | %s", m))
    await executor.start()
    try:
        rec, _, _ = await service.submit(SubmitRequest(
            idempotency_key="ov-4", task_type="flaky",
            payload={"fail_times": 99}, request_id="req-ov4",
            max_retries=5, time_budget_seconds=0.05,
        ))
        rec = await service.wait_until_terminal(rec.id, timeout=5)
        assert rec.state is TaskState.FAILED
        assert rec.error_code == "budget_exhausted"
        assert rec.failure_kind == "budget_exhausted"
        assert rec.error_code != "timeout"
        assert rec.time_budget_seconds == 0.05
        # 预算在退避窗口内耗尽，尝试次数远小于 1+5
        assert rec.attempts < 6
        logger.info(
            "预算用尽 req=req-ov4 attempts=%s code=%s -> 与单次超时可区分",
            rec.attempts, rec.error_code,
        )
    finally:
        await executor.stop()


@pytest.mark.asyncio
async def test_overrides_visible_in_status_and_history(components):
    """覆盖取值在状态流转与查询结果中可见。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="ov-5", task_type="flaky",
        payload={"fail_times": 1}, request_id="req-ov5",
        max_retries=4, time_budget_seconds=30,
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.max_retries == 4
    assert rec.time_budget_seconds == 30
    # 重试说明里体现覆盖后的额度 (1/4)
    notes = [note for *_, note in rec.history]
    assert any("(1/4)" in note for note in notes)
    logger.info(
        "覆盖可见 req=req-ov5 max_retries=%s budget=%s notes=%s",
        rec.max_retries, rec.time_budget_seconds, notes,
    )


@pytest.mark.asyncio
async def test_same_key_same_overrides_replay_same_task(components):
    """同键+同内容+同覆盖：复用同一任务，不重复执行、不重复副作用。"""
    svc = components.service
    kwargs = dict(
        idempotency_key="ov-6", task_type="side_effect",
        payload={"value": "z"}, max_retries=3, time_budget_seconds=10,
    )
    rec1, created1, _ = await svc.submit(
        SubmitRequest(request_id="req-ov6a", **kwargs))
    await svc.wait_until_terminal(rec1.id, timeout=5)
    rec2, created2, reason2 = await svc.submit(
        SubmitRequest(request_id="req-ov6b", **kwargs))
    assert created1 is True
    assert created2 is False and reason2 == "duplicate_replay"
    assert rec2.id == rec1.id
    assert components.effects.count(rec1.id) == 1


@pytest.mark.asyncio
async def test_same_key_different_overrides_rejected(components):
    """同键但覆盖取值不同：视为不同内容，明确拒绝（不静默复用）。"""
    from app.errors import IdempotencyConflictError

    svc = components.service
    await svc.submit(SubmitRequest(
        idempotency_key="ov-7", task_type="flaky", payload={},
        request_id="req-ov7a", max_retries=1,
    ))
    with pytest.raises(IdempotencyConflictError):
        await svc.submit(SubmitRequest(
            idempotency_key="ov-7", task_type="flaky", payload={},
            request_id="req-ov7b", max_retries=2,
        ))
    with pytest.raises(IdempotencyConflictError):
        await svc.submit(SubmitRequest(
            idempotency_key="ov-7", task_type="flaky", payload={},
            request_id="req-ov7c", time_budget_seconds=5,
        ))
    logger.info("覆盖冲突 key=ov-7 不同覆盖 -> 409 语义拒绝")


@pytest.mark.asyncio
async def test_http_invalid_override_422(httpx_client):
    """HTTP 层：非法覆盖返回 422 与可区分错误码，不是 500。"""
    resp = await httpx_client.post("/tasks", json={
        "idempotency_key": "ov-http", "task_type": "flaky",
        "payload": {}, "max_retries": 99,
    }, headers={"x-request-id": "req-ovh1"})
    assert resp.status_code == 422
    body = resp.json()
    assert body["error_code"] == "invalid_request_parameter"
    assert "max_retries" in body["message"]
    assert body["request_id"] == "req-ovh1"
    logger.info("HTTP 非法覆盖 req=req-ovh1 -> 422 %s", body["error_code"])


@pytest.mark.asyncio
async def test_http_overrides_visible_in_view(httpx_client):
    """HTTP 层：覆盖后的取值在提交响应与查询视图中可见。"""
    resp = await httpx_client.post("/tasks", json={
        "idempotency_key": "ov-http2", "task_type": "flaky",
        "payload": {"fail_times": 1}, "max_retries": 3,
        "time_budget_seconds": 20,
    }, headers={"x-request-id": "req-ovh2"})
    assert resp.status_code == 202
    task = resp.json()["task"]
    assert task["max_retries"] == 3
    assert task["time_budget_seconds"] == 20
    got = await httpx_client.get(f"/tasks/by-key/ov-http2")
    assert got.json()["max_retries"] == 3
    assert got.json()["time_budget_seconds"] == 20
