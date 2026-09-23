"""HTTP 入口集成测试：状态码、错误码区分与 X-Request-ID / X-Trace-ID 透传。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from task_api.api import create_app
from task_api.config import Settings
from task_api.executor import TaskExecutor
from task_api.handlers import HandlerRegistry
from task_api.service import TaskService
from task_api.storage import TaskStore


@pytest.fixture
def client():
    settings = Settings(
        max_concurrency=1,
        queue_capacity=1,
        default_max_attempts=1,
        retry_backoff_base_seconds=0.001,
        default_timeout_seconds=2.0,
    )
    store = TaskStore()
    registry = HandlerRegistry()

    async def echo(payload, ctx):
        return {"ok": True}

    async def perm(payload, ctx):
        from task_api.errors import PermanentTaskError

        raise PermanentTaskError("nope")

    registry.register("echo", echo)
    registry.register("perm", perm)
    executor = TaskExecutor(store, registry, settings)
    service = TaskService(store, executor, registry, settings)
    app = create_app(service)
    with TestClient(app) as c:
        c.service = service
        yield c


def _submit(client, key, task_type="echo", payload=None, headers=None):
    return client.post(
        "/tasks",
        json={
            "task_type": task_type,
            "payload": payload or {},
            "idempotency_key": key,
        },
        headers=headers or {},
    )


def test_submit_and_get_roundtrip_with_trace_headers(client):
    resp = _submit(
        client, "k1", headers={"X-Request-ID": "req-abc", "X-Trace-ID": "trace-xyz"}
    )
    assert resp.status_code == 202
    assert resp.headers["x-request-id"] == "req-abc"
    assert resp.headers["x-trace-id"] == "trace-xyz"
    body = resp.json()
    assert body["request_id"] == "req-abc"
    assert body["trace_id"] == "trace-xyz"
    task_id = body["task_id"]

    detail = client.get(f"/tasks/{task_id}")
    assert detail.status_code == 200
    assert detail.json()["status"] == "SUCCEEDED"


def test_duplicate_submit_http_returns_same_task(client):
    first = _submit(client, "dup")
    task_id = first.json()["task_id"]
    second = _submit(client, "dup")
    assert second.status_code == 202
    assert second.json()["task_id"] == task_id


def test_idempotency_conflict_maps_to_409(client):
    _submit(client, "conf", payload={"v": 1})
    resp = _submit(client, "conf", payload={"v": 2})
    assert resp.status_code == 409
    err = resp.json()["error"]
    assert err["code"] == "IDEMPOTENCY_KEY_CONFLICT"


def test_queue_full_maps_to_503_with_retry_after(client):
    import asyncio

    # 占住唯一并发槽位：用一个阻塞处理器
    async def hang(payload, ctx):
        await asyncio.Event().wait()

    client_executor = client.app  # noqa: F841
    # 直接通过 service 注册阻塞处理器并占满
    service = client.service
    service._handlers.register("hang", hang)

    r1 = _submit(client, "hang-1", task_type="hang")
    r2 = _submit(client, "hang-2", task_type="hang")
    assert r1.status_code == 202
    assert r2.status_code == 202  # 第二个进入容量为 1 的等待队列

    r3 = _submit(client, "hang-3", task_type="hang")
    assert r3.status_code == 503
    assert r3.json()["error"]["code"] == "QUEUE_FULL"
    assert r3.headers.get("retry-after") == "1"


def test_task_not_found_maps_to_404(client):
    resp = client.get("/tasks/task_missing")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "TASK_NOT_FOUND"


def test_permanent_failure_visible_in_status_with_distinct_code(client):
    resp = _submit(client, "perm-1", task_type="perm")
    task_id = resp.json()["task_id"]
    detail = client.get(f"/tasks/{task_id}")
    body = detail.json()
    assert body["status"] == "FAILED"
    assert body["error_code"] == "PERMANENT_FAILURE"
    assert body["failure_class"] == "PERMANENT"
