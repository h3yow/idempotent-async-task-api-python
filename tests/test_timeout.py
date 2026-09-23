"""超时分类测试：超时属于瞬时失败，受重试策略约束。"""

from __future__ import annotations

import logging

import pytest

from app.config import Settings
from app.handlers import HandlerRegistry
from app.models import SubmitRequest, TaskState
from app.wiring import build_service

from .handlers_helpers import install_test_handlers, EffectLog

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_timeout_classified_transient_and_retried():
    settings = Settings(
        max_concurrency=1, max_queue_size=4,
        task_timeout_seconds=0.1, max_retries=2,
    )
    effects = EffectLog()
    handlers = HandlerRegistry()
    install_test_handlers(handlers, effects)
    service, executor, _ = build_service(
        settings, handlers=handlers,
        log=lambda m: logger.info("    | %s", m))
    await executor.start()
    try:
        rec, _, _ = await service.submit(SubmitRequest(
            idempotency_key="timeout-1", task_type="timeout_then_ok",
            payload={"sleep": 0.3}, request_id="req-timeout1",
        ))
        rec = await service.wait_until_terminal(rec.id, timeout=5)
        assert rec.state is TaskState.SUCCEEDED
        assert rec.attempts == 3
        notes = [note for *_, note in rec.history]
        assert "transient(timeout); retry later (1/2)" in notes
        assert "transient(timeout); retry later (2/2)" in notes
        assert notes[-1] == "attempt 3 ok"
        logger.info(
            "超时重试 req=req-timeout1 attempts=%s -> 超时归类瞬时并重试成功",
            rec.attempts,
        )
    finally:
        await executor.stop()
