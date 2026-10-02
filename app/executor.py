"""任务执行器：并发槽位调度、超时、瞬时失败退避重试、取消。

调度不变量：
- 任意时刻 RUNNING 任务数 <= settings.max_concurrency；
- 一个任务记录最多对应一条执行链（重复提交复用同一记录，不重复执行）；
- 每次状态流转必须合法，且记录到 history 以便追踪；
- 瞬时失败重试时释放槽位、重新入 FIFO 队列，不与排队任务争抢额度。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from .config import Settings
from .errors import (
    BudgetExhaustedError,
    PermanentError,
    SideEffectCommittedError,
    TaskCancelled,
    TaskTimeoutError,
    TransientError,
    UnknownTaskTypeError,
)
from .handlers import HandlerRegistry, TaskContext
from .models import TERMINAL_STATES, TaskRecord, TaskState
from .store import TaskStore


class TaskExecutor:
    def __init__(
        self,
        store: TaskStore,
        handlers: HandlerRegistry,
        settings: Settings,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self._store = store
        self._handlers = handlers
        self._settings = settings
        self._log_fn = log
        # 每个任务一条执行链，防止重复调度
        self._chains: dict[str, asyncio.Task] = {}
        # 任务级串行锁：终结/取消与执行链竞争时只允许一方完成流转
        self._chain_locks: dict[str, asyncio.Lock] = {}
        # 取消信号
        self._cancel_events: dict[str, asyncio.Event] = {}
        # 终态通知
        self._terminal_events: dict[str, asyncio.Event] = {}
        self._wakeup = asyncio.Event()
        self._dispatcher: asyncio.Task | None = None
        self._stopping = False

    # ---------- 基础工具 ----------

    def _log(self, message: str) -> None:
        if self._log_fn is not None:
            self._log_fn(message)

    def _cancel_event(self, task_id: str) -> asyncio.Event:
        event = self._cancel_events.get(task_id)
        if event is None:
            event = asyncio.Event()
            self._cancel_events[task_id] = event
        return event

    def _terminal_event(self, task_id: str) -> asyncio.Event:
        event = self._terminal_events.get(task_id)
        if event is None:
            event = asyncio.Event()
            self._terminal_events[task_id] = event
        return event

    def _chain_lock(self, task_id: str) -> asyncio.Lock:
        lock = self._chain_locks.get(task_id)
        if lock is None:
            lock = asyncio.Lock()
            self._chain_locks[task_id] = lock
        return lock

    def notify(self) -> None:
        """唤醒调度循环（提交/重试入队后调用）。"""
        self._wakeup.set()

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        """启动调度循环。"""
        if self._dispatcher is None:
            self._stopping = False
            self._dispatcher = asyncio.create_task(self._dispatch_loop())
            self._log("executor started")

    async def stop(self) -> None:
        """优雅停止：停止调度，等待运行中任务结束。"""
        self._stopping = True
        self.notify()
        if self._dispatcher is not None:
            self._dispatcher.cancel()
            try:
                await self._dispatcher
            except asyncio.CancelledError:
                pass
            self._dispatcher = None
        chains = list(self._chains.values())
        if chains:
            await asyncio.gather(*chains, return_exceptions=True)
        self._log("executor stopped")

    # ---------- 取消 ----------

    async def cancel(self, task_id: str) -> TaskRecord:
        """取消排队中或执行中的任务，流转到 CANCELLED。

        - 排队中：立即从队列摘除并 CANCELLED；
        - 执行中：发出取消信号，由执行链在当前尝试边界完成 CANCELLED 流转；
        - 已终态：原样返回（取消幂等，不报错）。
        """
        record = self._store.get(task_id)
        if record is None:
            raise KeyError(task_id)
        if record.state in TERMINAL_STATES:
            return record

        self._cancel_event(task_id).set()
        async with self._chain_lock(task_id), self._store.lock:
            # 双重检查：拿到锁后状态可能已变化
            if record.state in TERMINAL_STATES:
                return record
            self._store.remove_from_queue(task_id)
            if record.state is TaskState.PENDING:
                record.transition(TaskState.CANCELLED, "cancel while queued")
                self._terminal_event(task_id).set()
                self._log(self._fmt(record, "CANCELLED", "排队中取消"))
            # RUNNING 情况下由执行链完成流转
        self.notify()

        # 等待执行链观察到取消并终结
        chain = self._chains.get(task_id)
        if chain is not None and not chain.done():
            await chain
        return record

    async def wait_until_terminal(
        self, task_id: str, timeout: float | None = None
    ) -> TaskRecord:
        """等待任务到达终态（测试/同步查询使用）。"""
        record = self._store.get(task_id)
        if record is None:
            raise KeyError(task_id)
        event = self._terminal_event(task_id)
        if record.state not in TERMINAL_STATES:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        return record

    # ---------- 调度 ----------

    def spawn_direct(self, task_id: str) -> None:
        """提交时有空闲槽位：已占槽，直接启动执行链。"""
        if task_id in self._chains and not self._chains[task_id].done():
            return
        self._chains[task_id] = asyncio.create_task(self._run_chain(task_id))

    async def _dispatch_loop(self) -> None:
        """单调度循环：有槽位时按 FIFO 取队首任务启动执行链。

        单一调度者 + 提交路径的 spawn_direct 共同保证并发额度不超卖：
        槽位的占用（mark_running）与队列读取在同一把锁内原子完成。
        """
        while not self._stopping:
            self._wakeup.clear()
            launched: list[str] = []
            async with self._store.lock:
                while self._store.has_capacity():
                    task_id = self._store.dequeue_one()
                    if task_id is None:
                        break
                    self._store.mark_running(task_id)
                    chain = asyncio.create_task(self._run_chain(task_id))
                    self._chains[task_id] = chain
                    launched.append(task_id)
            if launched:
                self._log(f"dispatcher launched {launched}")
            await self._wakeup.wait()

    # ---------- 执行链 ----------

    def _fmt(self, record: TaskRecord, to_state: str, note: str) -> str:
        return (
            f"[req={record.request_id} task={record.id} type={record.task_type} "
            f"attempt={record.attempts} running={self._store.running_count()} "
            f"queue={self._store.queue_size()} -> {to_state}] {note}"
        )

    async def _run_chain(self, task_id: str) -> None:
        """单任务执行链：执行 -> （瞬时失败）重新排队重试 -> 终态。"""
        record = self._store.get(task_id)
        if record is None:
            return
        try:
            while True:
                cancel_event = self._cancel_event(task_id)
                if cancel_event.is_set():
                    async with self._chain_lock(task_id):
                        if record.state is TaskState.PENDING:
                            record.transition(TaskState.CANCELLED, "cancel before attempt")
                            self._terminal_event(task_id).set()
                            self._log(self._fmt(record, "CANCELLED", "取消信号已置位"))
                    return

                # 整体时间预算：在发起新尝试前检查，预算用尽即终结，
                # 与单次执行超时（timeout）是可区分的失败原因
                if record.deadline_at is not None and time.time() >= record.deadline_at:
                    async with self._chain_lock(task_id):
                        if record.state is TaskState.PENDING:
                            record.error_code = BudgetExhaustedError.code
                            record.error_message = (
                                f"整体时间预算用尽 ({record.time_budget_seconds}s)"
                            )
                            record.failure_kind = "budget_exhausted"
                            record.transition(
                                TaskState.FAILED, "budget exhausted before attempt"
                            )
                            self._terminal_event(task_id).set()
                            self._log(
                                self._fmt(record, "FAILED", "整体时间预算用尽，不再尝试")
                            )
                    return

                record.attempts += 1
                attempt = record.attempts
                async with self._chain_lock(task_id):
                    if record.state is not TaskState.PENDING:
                        # 可能已在持锁前被取消
                        return
                    record.transition(TaskState.RUNNING, f"attempt {attempt} start")
                self._log(self._fmt(record, "RUNNING", f"第 {attempt} 次尝试开始"))

                ctx = TaskContext(
                    attempts=attempt, cancelled=cancel_event.is_set(), _record=record
                )
                ok, err_code, err_msg, result = await self._execute_once(record, ctx)

                async with self._chain_lock(task_id):
                    if cancel_event.is_set() and record.state is TaskState.RUNNING:
                        record.error_code = TaskCancelled.code
                        record.error_message = "任务被取消"
                        record.transition(TaskState.CANCELLED, "cancel during attempt")
                        self._terminal_event(task_id).set()
                        self._log(self._fmt(record, "CANCELLED", "执行中取消"))
                        return

                    if ok:
                        record.result = result
                        record.error_code = None
                        record.error_message = None
                        record.transition(TaskState.SUCCEEDED, f"attempt {attempt} ok")
                        self._terminal_event(task_id).set()
                        self._log(self._fmt(record, "SUCCEEDED", f"第 {attempt} 次尝试成功"))
                        return

                    # 失败分类：瞬时失败且还有重试额度 => 重新排队；否则终结
                    assert err_code is not None
                    record.error_code = err_code
                    record.error_message = err_msg

                    # 副作用保护：不可重复的动作已实际生效，任何失败/超时都
                    # 不得再触发重试，以 ABORTED 终态提前结束（区别于 FAILED）
                    if record.side_effect_committed:
                        record.failure_kind = "side_effect_committed"
                        record.error_code = SideEffectCommittedError.code
                        record.error_message = (
                            f"副作用已生效但任务未完成，已停止重试，需人工/补偿处理"
                            f"（原始失败: {err_code}: {err_msg}）"
                        )
                        record.transition(
                            TaskState.ABORTED,
                            f"side effect committed; abort on {err_code}",
                        )
                        self._terminal_event(task_id).set()
                        self._log(
                            self._fmt(
                                record,
                                "ABORTED",
                                "副作用已生效，提前终止（不再重试）",
                            )
                        )
                        return

                    retries_used = attempt - 1
                    budget_exhausted = (
                        record.deadline_at is not None
                        and time.time() >= record.deadline_at
                    )
                    can_retry = (
                        err_code in (TransientError.code, TaskTimeoutError.code)
                        and retries_used < record.max_retries
                        and not budget_exhausted
                    )
                    if can_retry:
                        record.failure_kind = "transient"
                        record.transition(
                            TaskState.PENDING,
                            f"transient({err_code}); retry later "
                            f"({retries_used + 1}/{record.max_retries})",
                        )
                        # 槽位释放与重新入队原子完成（锁序：chain_lock -> store.lock）
                        async with self._store.lock:
                            self._store.mark_finished(task_id)
                            self._store.enqueue(task_id)
                        self._log(
                            self._fmt(
                                record,
                                "PENDING",
                                f"瞬时失败 {err_code}，重新排队重试",
                            )
                        )
                        # 退避后再唤醒调度，避免失败任务立即占回槽位；
                        # 退避期间可被取消打断；退避不超过剩余整体预算。
                        backoff = self._settings.retry_backoff_seconds * (
                            retries_used + 1
                        )
                        if record.deadline_at is not None:
                            backoff = min(
                                backoff, max(0.0, record.deadline_at - time.time())
                            )
                        try:
                            await asyncio.wait_for(
                                self._cancel_event(task_id).wait(),
                                timeout=backoff,
                            )
                        except asyncio.TimeoutError:
                            pass
                        if self._cancel_event(task_id).is_set():
                            # 此处仍持有 chain_lock（外层临界区），只需再取 store.lock
                            async with self._store.lock:
                                if record.state is TaskState.PENDING:
                                    self._store.remove_from_queue(task_id)
                                    record.transition(
                                        TaskState.CANCELLED, "cancel during backoff"
                                    )
                                    self._terminal_event(task_id).set()
                                    self._log(
                                        self._fmt(record, "CANCELLED", "退避期间取消")
                                    )
                                    return
                        self.notify()
                        return  # 等待 dispatcher 重新调度
                    if err_code == BudgetExhaustedError.code or (
                        budget_exhausted
                        and err_code in (TransientError.code, TaskTimeoutError.code)
                    ):
                        # 整体预算用尽：与单次超时/重试耗尽区分的失败原因
                        record.failure_kind = "budget_exhausted"
                        record.error_code = BudgetExhaustedError.code
                        record.error_message = (
                            f"整体时间预算用尽 ({record.time_budget_seconds}s)，"
                            f"停止重试（原始失败: {err_code}: {err_msg}）"
                        )
                        record.transition(
                            TaskState.FAILED, "budget exhausted; stop retrying"
                        )
                    else:
                        record.failure_kind = (
                            "transient"
                            if err_code in (TransientError.code, TaskTimeoutError.code)
                            else "permanent"
                        )
                        record.transition(
                            TaskState.FAILED,
                            f"{record.failure_kind}({err_code}); retries exhausted"
                            if record.failure_kind == "transient"
                            else f"permanent({err_code}); no retry",
                        )
                    self._terminal_event(task_id).set()
                    self._log(self._fmt(record, "FAILED", f"{record.failure_kind}: {err_msg}"))
                    return
        finally:
            # 到达终态（成功/失败/取消/中止）后释放槽位；重试路径已自行释放并入队
            async with self._store.lock:
                if record.state in TERMINAL_STATES:
                    self._store.mark_finished(task_id)
            self.notify()

    async def _execute_once(
        self, record: TaskRecord, ctx: TaskContext
    ) -> tuple[bool, str | None, str | None, object]:
        """执行单次尝试，施加超时；返回 (是否成功, 错误码, 错误信息, 结果)。"""
        try:
            handler = self._handlers.get(record.task_type)
        except UnknownTaskTypeError as exc:
            return False, exc.code, str(exc), None

        # 单次尝试的超时阈值受整体时间预算约束：剩余预算更少时以预算为准，
        # 超时时按「预算用尽」而非「单次超时」归类，两者保持可区分。
        timeout = self._settings.task_timeout_seconds
        if record.deadline_at is not None:
            remaining = record.deadline_at - time.time()
            if remaining <= 0:
                return False, BudgetExhaustedError.code, (
                    f"整体时间预算用尽 ({record.time_budget_seconds}s)"
                ), None
            timeout = min(timeout, remaining)

        try:
            result = await asyncio.wait_for(
                handler(record, ctx),
                timeout=timeout,
            )
            return True, None, None, result
        except asyncio.TimeoutError:
            if record.deadline_at is not None and time.time() >= record.deadline_at:
                return False, BudgetExhaustedError.code, (
                    f"整体时间预算用尽 ({record.time_budget_seconds}s)"
                ), None
            return False, TaskTimeoutError.code, (
                f"执行超时 ({self._settings.task_timeout_seconds}s)"
            ), None
        except (TransientError, PermanentError) as exc:
            kind = "transient" if isinstance(exc, TransientError) else "permanent"
            return False, exc.code, str(exc) or kind, None
        except asyncio.CancelledError:
            # 任务级取消信号置位时 wait_for 也可能被外层取消；归为取消
            if self._cancel_event(record.id).is_set():
                return False, TaskCancelled.code, "任务被取消", None
            raise
        except Exception as exc:  # 未声明的异常按瞬时失败处理，允许一次重试
            return False, TransientError.code, f"unexpected: {exc!r}", None
