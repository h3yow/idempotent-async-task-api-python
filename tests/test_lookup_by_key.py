"""幂等键反查：客户端丢失响应后可凭同一键查回任务状态。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from .handlers_helpers import make_holder

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_lookup_by_key_returns_current_state(httpx_client):
    """反查返回状态、尝试次数、失败原因、副作用与重试标志。"""
    resp = await httpx_client.post("/tasks", json={
        "idempotency_key": "lk-1", "task_type": "side_effect",
        "payload": {"value": "v"}}, headers={"x-request-id": "req-lk1"})
    assert resp.status_code == 202
    task_id = resp.json()["task"]["task_id"]

    for _ in range(50):
        got = await httpx_client.get("/tasks/by-key/lk-1",
                                     headers={"x-request-id": "req-lk1q"})
        if got.json()["state"] == "SUCCEEDED":
            break
        await asyncio.sleep(0.02)
    body = got.json()
    assert got.status_code == 200
    assert body["task_id"] == task_id
    assert body["idempotency_key"] == "lk-1"
    assert body["state"] == "SUCCEEDED"
    assert body["attempts"] == 1
    assert body["side_effect_committed"] is False
    assert body["will_retry"] is False
    logger.info(
        "反查 req=req-lk1q key=lk-1 task=%s state=%s attempts=%s -> 与按 ID 查询一致",
        task_id, body["state"], body["attempts"],
    )


@pytest.mark.asyncio
async def test_lookup_by_key_not_found_is_distinct_404(httpx_client):
    """查无此键：明确 404 task_not_found，不是 500 也不是空响应。"""
    resp = await httpx_client.get("/tasks/by-key/never-existed",
                                  headers={"x-request-id": "req-lk404"})
    assert resp.status_code == 404
    body = resp.json()
    assert body["error_code"] == "task_not_found"
    assert "never-existed" in body["message"]
    assert "current_concurrency" in body and "queue_depth" in body
    logger.info("反查未命中 req=req-lk404 code=%s -> 404 可区分", body["error_code"])


@pytest.mark.asyncio
async def test_lookup_by_key_is_pure_query(httpx_client, components):
    """反查不触发任何执行或副作用：排队中的任务查完仍在排队。"""
    keys = ["lk-hold-0", "lk-hold-1"]
    holders = [make_holder(k) for k in keys]
    try:
        for k in keys:  # 占满两个槽位
            await httpx_client.post("/tasks", json={
                "idempotency_key": k, "task_type": "slow", "payload": {}})
        queued = await httpx_client.post("/tasks", json={
            "idempotency_key": "lk-queued", "task_type": "side_effect",
            "payload": {}})
        assert queued.json()["reason"] == "created_queued"

        for _ in range(3):  # 多次反查都是纯查询
            got = await httpx_client.get("/tasks/by-key/lk-queued")
            assert got.status_code == 200
            body = got.json()
            assert body["state"] == "PENDING"
            assert body["attempts"] == 0
            assert body["will_retry"] is True
        assert components.effects.count(body["task_id"]) == 0
        logger.info(
            "纯查询 key=lk-queued task=%s state=%s attempts=%s 副作用次数=0 -> 不触发执行",
            body["task_id"], body["state"], body["attempts"],
        )
    finally:
        for h in holders:
            h.set()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_lookup_by_key_reflects_aborted_side_effect(httpx_client):
    """反查能看出 ABORTED：副作用已生效、不会再重试、原因可与普通失败区分。"""
    await httpx_client.post("/tasks", json={
        "idempotency_key": "lk-abort", "task_type": "commit_then_fail",
        "payload": {"after_commit": "transient"}},
        headers={"x-request-id": "req-lka"})
    for _ in range(50):
        got = await httpx_client.get("/tasks/by-key/lk-abort")
        if got.json()["state"] == "ABORTED":
            break
        await asyncio.sleep(0.02)
    body = got.json()
    assert body["state"] == "ABORTED"
    assert body["failure_kind"] == "side_effect_committed"
    assert body["error_code"] == "side_effect_committed"
    assert body["side_effect_committed"] is True
    assert body["will_retry"] is False
    logger.info(
        "反查 ABORTED key=lk-abort kind=%s side_effect=%s will_retry=%s -> 需人工/补偿",
        body["failure_kind"], body["side_effect_committed"], body["will_retry"],
    )


@pytest.mark.asyncio
async def test_lookup_does_not_weaken_conflict_rule(httpx_client):
    """反查存在后，同键不同内容仍然 409（拒绝规则不被破坏）。"""
    await httpx_client.post("/tasks", json={
        "idempotency_key": "lk-conflict", "task_type": "side_effect",
        "payload": {"v": 1}})
    got = await httpx_client.get("/tasks/by-key/lk-conflict")
    assert got.status_code == 200
    conflict = await httpx_client.post("/tasks", json={
        "idempotency_key": "lk-conflict", "task_type": "side_effect",
        "payload": {"v": 2}})
    assert conflict.status_code == 409
    assert conflict.json()["error_code"] == "idempotency_content_conflict"
    logger.info("反查后冲突规则不变 key=lk-conflict -> 409")
