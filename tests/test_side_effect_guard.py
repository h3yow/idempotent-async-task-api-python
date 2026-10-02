"""副作用保护：不可重复动作一旦生效，任何后续失败都不得再触发重试。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.models import SubmitRequest, TaskState

from .handlers_helpers import make_holder

logger = logging.getLogger("task_tests")


@pytest.mark.asyncio
async def test_committed_side_effect_not_retried_on_transient(components):
    """动作生效后遇到瞬时失败：ABORTED 终态，副作用只发生一次，不再重试。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="guard-1", task_type="commit_then_fail",
        payload={"after_commit": "transient"}, request_id="req-guard1",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.ABORTED
    assert rec.attempts == 1                      # 没有发起第二次尝试
    assert rec.side_effect_committed is True
    assert rec.failure_kind == "side_effect_committed"
    assert rec.error_code == "side_effect_committed"
    assert rec.will_retry is False
    # 关键断言：不可重复的动作只生效了一次
    assert components.effects.count(rec.id) == 1
    states = [(s.value, d.value) for _, _, s, d, _ in rec.history]
    assert states[-1] == ("RUNNING", "ABORTED")
    assert ("RUNNING", "PENDING") not in states   # 未曾重新排队
    logger.info(
        "副作用保护 req=req-guard1 task=%s attempts=%s 副作用次数=%s 流转=%s -> ABORTED",
        rec.id, rec.attempts, components.effects.count(rec.id), states,
    )


@pytest.mark.asyncio
async def test_committed_side_effect_not_retried_on_timeout(components):
    """动作生效后单次执行超时：同样 ABORTED，不得通过重试把动作做第二遍。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="guard-2", task_type="commit_then_fail",
        payload={"after_commit": "timeout", "sleep": 5.0},
        request_id="req-guard2",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.ABORTED
    assert rec.attempts == 1
    assert rec.failure_kind == "side_effect_committed"
    assert components.effects.count(rec.id) == 1
    logger.info(
        "生效后超时 req=req-guard2 task=%s attempts=%s 副作用次数=%s -> ABORTED 不重试",
        rec.id, rec.attempts, components.effects.count(rec.id),
    )


@pytest.mark.asyncio
async def test_failure_before_commit_retries_normally(components):
    """失败发生在动作生效之前：按原规则正常重试，不误伤。"""
    svc = components.service
    rec, _, _ = await svc.submit(SubmitRequest(
        idempotency_key="guard-3", task_type="commit_then_fail",
        payload={"fail_before_commit": 2, "after_commit": "ok"},
        request_id="req-guard3",
    ))
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.SUCCEEDED
    assert rec.attempts == 3                      # 前两次生效前失败被正常重试
    assert rec.side_effect_committed is True
    # 动作只在第三次（成功那次）生效，恰好一次
    assert components.effects.attempts(rec.id) == [3]
    logger.info(
        "生效前失败 req=req-guard3 task=%s attempts=%s 副作用次数=%s -> 正常重试后成功",
        rec.id, rec.attempts, components.effects.count(rec.id),
    )


@pytest.mark.asyncio
async def test_aborted_task_releases_concurrency_slot(components):
    """ABORTED 提前终结后并发名额正常归还：排队任务能继续跑，不卡、不漏还。"""
    svc = components.service
    holder = make_holder("guard-slot-hold")
    try:
        # 占满两个槽位：一个 slow 占住，一个即将 ABORTED
        slow_rec, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="guard-slot-hold", task_type="slow", payload={},
            request_id="req-gs0"))
        abort_rec, _, _ = await svc.submit(SubmitRequest(
            idempotency_key="guard-slot-abort", task_type="commit_then_fail",
            payload={"after_commit": "transient"}, request_id="req-gs1"))
        # 槽位已满，第三个任务排队
        queued, _, reason = await svc.submit(SubmitRequest(
            idempotency_key="guard-slot-queued", task_type="side_effect",
            payload={"value": "q"}, request_id="req-gs2"))
        assert reason == "created_queued"

        aborted = await svc.wait_until_terminal(abort_rec.id, timeout=5)
        assert aborted.state is TaskState.ABORTED
        # ABORTED 释放槽位后，排队任务应被调度并成功
        done = await svc.wait_until_terminal(queued.id, timeout=5)
        assert done.state is TaskState.SUCCEEDED
        assert components.effects.count(queued.id) == 1
        # 只剩 slow 还占着一个槽位
        assert components.store.running_count() == 1
        logger.info(
            "槽位归还 req=req-gs1 aborted=%s queued_task=%s state=%s running=%s -> 名额已还",
            aborted.id, queued.id, done.state.value,
            components.store.running_count(),
        )
        assert slow_rec.id != aborted.id
    finally:
        holder.set()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_aborted_task_duplicate_replay_does_not_reexecute(components):
    """ABORTED 后同键同内容重放：复用同一记录，绝不把动作再做一遍。"""
    svc = components.service
    req = SubmitRequest(
        idempotency_key="guard-4", task_type="commit_then_fail",
        payload={"after_commit": "transient"}, request_id="req-g4a",
    )
    rec, _, _ = await svc.submit(req)
    rec = await svc.wait_until_terminal(rec.id, timeout=5)
    assert rec.state is TaskState.ABORTED

    replay, created, reason = await svc.submit(SubmitRequest(
        idempotency_key="guard-4", task_type="commit_then_fail",
        payload={"after_commit": "transient"}, request_id="req-g4b",
    ))
    assert created is False and reason == "duplicate_replay"
    assert replay.id == rec.id
    assert replay.state is TaskState.ABORTED
    assert replay.attempts == 1
    assert components.effects.count(rec.id) == 1   # 重放没有产生第二次副作用
    logger.info(
        "ABORTED 重放 req=req-g4b task=%s 副作用次数=%s -> 复用记录不重复执行",
        rec.id, components.effects.count(rec.id),
    )
