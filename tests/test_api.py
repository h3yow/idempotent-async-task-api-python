"""HTTP 层测试：不同入口（重复提交/过载/冲突）返回可区分结果。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from .handlers_helpers import make_holder

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_http_submit_and_get(httpx_client):
    resp = await httpx_client.post("/tasks", json={
        "idempotency_key": "http-1",
        "task_type": "side_effect",
        "payload": {"value": "hello"},
    }, headers={"x-request-id": "req-http1"})
    assert resp.status_code == 202
    body = resp.json()
    assert body["created"] is True
    assert body["reason"] == "created_running"
    assert body["current_concurrency"] == 1
    task_id = body["task"]["task_id"]

    for _ in range(50):
        got = await httpx_client.get(f"/tasks/{task_id}")
        if got.json()["state"] == "SUCCEEDED":
            break
        await asyncio.sleep(0.02)
    assert got.json()["state"] == "SUCCEEDED"
    assert got.json()["result"] == {"attempt": 1, "echo": "hello"}
    assert got.json()["request_id"] == "req-http1"


@pytest.mark.asyncio
async def test_http_duplicate_replay(httpx_client):
    payload = {"idempotency_key": "http-2", "task_type": "side_effect",
               "payload": {"v": 1}}
    first = await httpx_client.post("/tasks", json=payload,
                                    headers={"x-request-id": "req-h2a"})
    assert first.status_code == 202
    task_id = first.json()["task"]["task_id"]
    second = await httpx_client.post("/tasks", json=payload,
                                     headers={"x-request-id": "req-h2b"})
    assert second.status_code == 202
    body = second.json()
    assert body["created"] is False
    assert body["reason"] == "duplicate_replay"
    assert body["task"]["task_id"] == task_id
    logger.info("HTTP 重放 req=req-h2b created=false -> 同一任务 %s", task_id)


@pytest.mark.asyncio
async def test_http_conflict_409(httpx_client):
    await httpx_client.post("/tasks", json={
        "idempotency_key": "http-3", "task_type": "side_effect",
        "payload": {"v": 1}}, headers={"x-request-id": "req-h3a"})
    resp = await httpx_client.post("/tasks", json={
        "idempotency_key": "http-3", "task_type": "side_effect",
        "payload": {"v": 2}}, headers={"x-request-id": "req-h3b"})
    assert resp.status_code == 409
    body = resp.json()
    assert body["error_code"] == "idempotency_content_conflict"
    assert body["request_id"] == "req-h3b"
    assert "current_concurrency" in body and "queue_depth" in body
    logger.info("HTTP 冲突 req=req-h3b code=%s -> 409 原因可区分", body["error_code"])


@pytest.mark.asyncio
async def test_http_queue_full_503(httpx_client, components):
    keys = [f"http-full-{i}" for i in range(5)]
    holders = [make_holder(k) for k in keys]
    try:
        for i, k in enumerate(keys[:4]):
            r = await httpx_client.post("/tasks", json={
                "idempotency_key": k, "task_type": "slow", "payload": {}},
                headers={"x-request-id": f"req-hf{i}"})
            assert r.status_code == 202
        rejected = await httpx_client.post("/tasks", json={
            "idempotency_key": keys[4], "task_type": "slow", "payload": {}},
            headers={"x-request-id": "req-hf4"})
        assert rejected.status_code == 503
        body = rejected.json()
        assert body["error_code"] == "capacity_queue_full"
        assert body["current_concurrency"] == 2
        assert body["queue_depth"] == 2
        assert rejected.headers.get("retry-after") == "1"
        logger.info(
            "HTTP 过载 req=req-hf4 code=%s concurrency=%s queue=%s -> 503",
            body["error_code"], body["current_concurrency"], body["queue_depth"],
        )
    finally:
        for h in holders[:4]:
            h.set()
        await asyncio.sleep(0.05)
