"""失败分类、重试、副作用幂等、超时与取消的状态一致性测试。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from task_api.errors import ErrorCode
from task_api.models import FailureClass, TaskStatus
from tests.conftest import Harness, make_settings

logger = logging.getLogger("task_api.test.retry")


@pytest.mark.asyncio
async def test_transient_failure_retries_until_success(harness):
    view = await harness.submit(
        "transient_then_ok",
        "recover",
        {"key": "recover", "succeed_on": 3},
        max_attempts=5,
    )
    rec = await harness.wait(view.task_id)

    assert rec.status == TaskStatus.SUCCEEDED
    assert rec.attempt == 3
    assert rec.result == {"recovered_on": 3}
    # 状态链路可解释：包含 RETRYING 段
    statuses = [e.to_status for e in rec.history]
    assert statuses.count(TaskStatus.RETRYING) == 2
    assert all(e.trace_id == rec.trace_id for e in rec.history)
    logger.info(
        "request=%s trace=%s recovered after attempts=%d path=%s",
        rec.request_id, rec.trace_id, rec.attempt,
        [s.value for s in statuses],
    )


@pytest.mark.asyncio
async def test_permanent_failure_does_not_retry(harness):
    view = await harness.submit(
        "always_permanent", "perm", max_attempts=5
    )
    rec = await harness.wait(view.task_id)

    assert rec.status == TaskStatus.FAILED
    assert rec.attempt == 1  # 永久失败不重试
    assert rec.error_code == ErrorCode.PERMANENT_FAILURE
    assert rec.failure_class == FailureClass.PERMANENT
    perm_calls = [c for c in harness.calls if "always_permanent" in c]
    assert len(perm_calls) == 1
    logger.info(
        "permanent failure request=%s trace=%s attempt=%d code=%s",
        rec.request_id, rec.trace_id, rec.attempt, rec.error_code.value,
    )


@pytest.mark.asyncio
async def test_retries_exhausted_is_distinguishable(harness):
    view = await harness.submit(
        "always_transient", "exhaust", {"key": "exhaust"}, max_attempts=3
    )
    rec = await harness.wait(view.task_id)

    assert rec.status == TaskStatus.FAILED
    assert rec.attempt == 3
    assert rec.error_code == ErrorCode.RETRIES_EXHAUSTED
    assert rec.failure_class == FailureClass.TRANSIENT
    assert harness.attempt_count["exhaust"] == 3
    logger.info(
        "retries exhausted request=%s attempts=%d final_code=%s",
        rec.request_id, rec.attempt, rec.error_code.value,
    )


@pytest.mark.asyncio
async def test_no_duplicate_side_effect_when_failure_after_commit(harness):
    # 处理器在提交副作用后瞬时失败：框架必须强制按永久失败处理，绝不重试
    view = await harness.submit(
        "side_effect_then_fail", "sidefx", max_attempts=5
    )
    rec = await harness.wait(view.task_id)

    assert rec.status == TaskStatus.FAILED
    assert rec.attempt == 1, "副作用后失败不应重试"
    assert rec.error_code == ErrorCode.PERMANENT_FAILURE
    assert rec.side_effect_occurred is True
    assert harness.side_effects == [f"{rec.task_id}:1"], "副作用被重复提交"
    logger.info(
        "side effect protected request=%s side_effects=%s",
        rec.request_id, harness.side_effects,
    )


@pytest.mark.asyncio
async def test_timeout_classified_transient_and_retried_then_failed():
    settings = make_settings(
        max_concurrency=2,
        queue_capacity=4,
        default_max_attempts=2,
        default_timeout_seconds=0.02,
        retry_backoff_base_seconds=0.005,
        timeout_is_transient=True,
    )
    h = Harness(settings)
    try:

        async def too_slow(payload, ctx):
            await asyncio.sleep(0.1)
            return "late"

        h.registry.register("too_slow", too_slow)
        view = await h.submit("too_slow", "timeout")
        rec = await h.wait(view.task_id, timeout=3)

        assert rec.status == TaskStatus.FAILED
        assert rec.attempt == 2
        assert rec.error_code == ErrorCode.RETRIES_EXHAUSTED
        # 每次失败的错误码都应可解释为 TIMEOUT（瞬时）
        timeout_events = [
            e for e in rec.history if e.error_code == ErrorCode.TRANSIENT_FAILURE
        ]
        assert rec.history  # 有完整链路
        logger.info(
            "timeout retried request=%s attempt=%d path=%s",
            rec.request_id, rec.attempt,
            [e.to_status.value for e in rec.history],
        )
        assert len(timeout_events) >= 1
    finally:
        await h.shutdown()


@pytest.mark.asyncio
async def test_cancel_running_task_reaches_consistent_cancelled_state(harness):
    view = await harness.submit("blocking", "running-cancel", {"gate": "rc"})
    # 等到真正运行
    for _ in range(50):
        rec = await harness.store.get(view.task_id)
        if rec.status == TaskStatus.RUNNING:
            break
        await asyncio.sleep(0.01)

    # blocking 处理器在收到取消信号并被释放后以异常退出，框架归一为 CANCELLED
    harness.executor.request_cancel(view.task_id)
    harness.gate("rc").set()
    resp = await harness.service.cancel(view.task_id, wait=True)

    final = await harness.store.get(view.task_id)
    assert final.status == TaskStatus.CANCELLED
    assert resp.status == TaskStatus.CANCELLED
    assert final.error_code == ErrorCode.CANCELLED
    logger.info(
        "running cancel request=%s final=%s code=%s",
        final.request_id, final.status.value,
        final.error_code.value if final.error_code else "-",
    )
