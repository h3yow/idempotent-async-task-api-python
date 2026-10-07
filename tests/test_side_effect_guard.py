"""副作用保护测试：不可重复动作一旦生效，绝不再重试。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.models import SubmitRequest, TaskState

from .handlers_helpers import make_holder

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_side_effect_then_permanent_failure_no_retry(components):
    """副作用生效后抛不可重试的永久失败：同样终结为 side_effect_committed。

    调用方从结果原因上必须能区分「动作已经做了、需要人工/补偿」
    和「什么都没发生就失败」（普通 permanent）。
    """
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="se-perm", task_type="sensitive",
        payload={"permanent_after_effect": True}, request_id="req-sep1",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 1                       # 永久失败也绝不再试一次
    assert rec.side_effect_occurred is True
    assert rec.failure_kind == "side_effect_committed"
    assert rec.error_code == "side_effect_committed"
    # 原始失败类型保留在错误信息里，便于人工排查
    assert "permanent_failure" in (rec.error_message or "")
    assert rec.will_retry() is False
    # 不可重复的动作只发生了一次
    assert components.effects.count(rec.id) == 1
    logger.info(
        "永久失败保护 req=req-sep1 task=%s attempts=%s effects=%s kind=%s -> "
        "与普通永久失败可区分",
        rec.id, rec.attempts, components.effects.count(rec.id), rec.failure_kind,
    )


@pytest.mark.asyncio
async def test_permanent_failure_without_side_effect_stays_permanent(components):
    """对照组：动作未生效时的永久失败仍是普通 permanent，原因不被拔高。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="se-perm0", task_type="flaky",
        payload={"permanent": True}, request_id="req-sep0",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.failure_kind == "permanent"
    assert rec.error_code == "permanent_failure"
    assert rec.side_effect_occurred is False
    assert components.effects.count(rec.id) == 0   # 什么都没发生
    logger.info(
        "对照组 req=req-sep0 task=%s kind=%s -> 未生效的永久失败保持原样",
        rec.id, rec.failure_kind,
    )


@pytest.mark.asyncio
async def test_side_effect_then_transient_failure_no_retry(components):
    """副作用生效后遇到瞬时失败：终结为 side_effect_committed，动作只发生一次。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="se-1", task_type="sensitive",
        payload={"fail_after_effect": True}, request_id="req-se1",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 1                       # 没有发生任何重试
    assert rec.side_effect_occurred is True
    assert rec.failure_kind == "side_effect_committed"
    assert rec.error_code == "side_effect_committed"
    assert rec.will_retry() is False
    # 不可重复的动作只发生了一次
    assert components.effects.count(rec.id) == 1
    # 与普通失败/重试耗尽可区分
    assert rec.failure_kind not in ("permanent", "transient")
    logger.info(
        "副作用保护 req=req-se1 task=%s attempts=%s effects=%s kind=%s -> 只生效一次",
        rec.id, rec.attempts, components.effects.count(rec.id), rec.failure_kind,
    )


@pytest.mark.asyncio
async def test_side_effect_then_timeout_no_retry(components):
    """副作用生效后单次执行超时：同样终结为 side_effect_committed，不重试。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="se-2", task_type="sensitive",
        payload={"sleep_after_effect": 5.0},  # 远超 task_timeout_seconds=1.0
        request_id="req-se2",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.FAILED
    assert rec.attempts == 1
    assert rec.failure_kind == "side_effect_committed"
    assert rec.error_code == "side_effect_committed"
    assert "timeout" in (rec.error_message or "")
    assert components.effects.count(rec.id) == 1
    logger.info(
        "超时保护 req=req-se2 task=%s attempts=%s effects=%s -> 超时也不重试",
        rec.id, rec.attempts, components.effects.count(rec.id),
    )


@pytest.mark.asyncio
async def test_failure_before_side_effect_still_retries(components):
    """失败发生在动作生效之前：按原规则正常重试，不误伤。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="se-3", task_type="sensitive",
        payload={"fail_before_effect": 2},  # 前两次在副作用生效前失败
        request_id="req-se3",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.attempts == 3
    assert rec.side_effect_occurred is True
    # 前两次失败发生在生效前，无副作用；动作只在第 3 次尝试发生一次
    assert components.effects.attempts(rec.id) == [3]
    logger.info(
        "生效前失败 req=req-se3 task=%s attempts=%s effects=%s -> 正常重试成功",
        rec.id, rec.attempts, components.effects.attempts(rec.id),
    )


@pytest.mark.asyncio
async def test_slot_released_after_side_effect_termination(components):
    """副作用保护提前终结后：并发名额正常归还，排队任务继续跑。"""
    svc = components.service
    h0 = make_holder("seg-0")
    h2 = make_holder("seg-2")
    try:
        # 占满两个槽位
        r0, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="seg-0", task_type="slow", payload={},
            request_id="req-seg0"))
        r1, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="seg-1", task_type="sensitive",
            # 生效后先占住槽位 0.2s 再失败，保证能观察到 r2 排队
            payload={"fail_after_effect": True, "sleep_after_effect": 0.2},
            request_id="req-seg1"))
        # 排队任务
        r2, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="seg-2", task_type="slow", payload={},
            request_id="req-seg2"))
        for _ in range(5):
            await asyncio.sleep(0)
        assert components.store.running_count() == 2
        assert (await svc.get(r2.id)).state is TaskState.PENDING

        # 敏感任务提前终结：名额归还，排队任务补位
        r1 = await svc.wait_until_terminal(r1.id, timeout=5)
        assert r1.failure_kind == "side_effect_committed"
        for _ in range(5):
            await asyncio.sleep(0)
        assert (await svc.get(r2.id)).state is TaskState.RUNNING
        assert components.store.running_count() == 2  # 没漏还也没超卖
        logger.info(
            "名额归还 req=req-seg1 running=%s queue=%s -> 排队任务已补位",
            components.store.running_count(), components.store.queue_size(),
        )
    finally:
        h0.set(); h2.set()
        await components.executor.wait_until_terminal(r0.id, timeout=3)
        await components.executor.wait_until_terminal(r2.id, timeout=3)
    # 全部结束后名额完全释放
    assert components.store.running_count() == 0


@pytest.mark.asyncio
async def test_duplicate_replay_of_side_effect_task_no_redo(components):
    """副作用任务终结后同键重放：复用记录，动作仍只发生一次。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="se-4", task_type="sensitive",
        payload={"fail_after_effect": True}, request_id="req-se4a",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.failure_kind == "side_effect_committed"

    rec2, created, reason = await svc.submit(SubmitRequest(
        idempotency_key="se-4", task_type="sensitive",
        payload={"fail_after_effect": True}, request_id="req-se4b",
    ))
    assert created is False and reason == "duplicate_replay"
    assert rec2.id == rec.id
    assert rec2.attempts == 1
    assert components.effects.count(rec.id) == 1
    logger.info(
        "重放保护 req=req-se4b task=%s effects=%s -> 重放不重复动作",
        rec2.id, components.effects.count(rec.id),
    )
