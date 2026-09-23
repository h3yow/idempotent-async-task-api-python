"""有界并发执行器：槽位预留 + FIFO 有界等待队列 + 退避重试。

采用"并发槽位（slot）"模型，所有计数只在同一把条件锁内变更：

- ``slots_taken``：已预留的并发槽位数（ready 待启动 + 运行中），取值 0..N；
- ``_ready``：已预留槽位、等待 worker 协程拉起的任务；
- ``_waiting``：槽位满时的有界 FIFO 等待队列（容量 ``queue_capacity``）。

提交时在临界区内**同步**裁决，结果确定、与时序无关：

1. ``slots_taken < max_concurrency``：立即预留一个槽位，进入 ready；
2. 槽位已满且等待队列未满：进入队尾，状态 QUEUED；
3. 槽位满且队列满：抛 ``QUEUE_FULL`` 明确拒绝，不静默丢弃。

运行结束（成功/失败/取消）释放槽位时，按 FIFO 把队首提升到 ready，
因此等待顺序由提交序号唯一确定，且同时运行数绝不超过上限。
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from datetime import timedelta
from typing import Any

from task_api.config import Settings
from task_api.errors import (
    ErrorCode,
    PermanentTaskError,
    QueueFull,
    TaskError,
    TaskTimeout,
    TransientTaskError,
)
from task_api.handlers import HandlerContext, HandlerRegistry
from task_api.logging_setup import get_logger, log_transition
from task_api.models import FailureClass, TaskRecord, TaskStatus, utcnow
from task_api.storage import TaskStore

logger = get_logger("executor")


class ConcurrencyStats:
    """并发度统计，供日志与诊断使用。"""

    def __init__(self) -> None:
        self.running = 0          # 真正在执行处理器的任务数
        self.slots_reserved = 0   # 已预留槽位（ready + running）
        self.waiting = 0          # 等待队列长度
        self.submitted = 0
        self.completed = 0
        self.rejected = 0
        self.max_running_seen = 0


class _AttemptOutcome:
    """单次尝试的结构化结果（成功/取消/瞬时失败/永久失败）。"""

    __slots__ = ("kind", "result", "error_code", "message", "failure_class")

    def __init__(
        self,
        kind: str,
        *,
        result: Any = None,
        error_code: ErrorCode = ErrorCode.TRANSIENT_FAILURE,
        message: str = "",
        failure_class: FailureClass = FailureClass.TRANSIENT,
    ) -> None:
        self.kind = kind
        self.result = result
        self.error_code = error_code
        self.message = message
        self.failure_class = failure_class

    @classmethod
    def success(cls, result: Any) -> "_AttemptOutcome":
        return cls("success", result=result)

    @classmethod
    def cancelled(cls, message: str) -> "_AttemptOutcome":
        return cls(
            "cancelled",
            error_code=ErrorCode.CANCELLED,
            message=message,
            failure_class=FailureClass.NONE,
        )

    @classmethod
    def transient(cls, exc: TaskError) -> "_AttemptOutcome":
        return cls(
            "failed",
            error_code=exc.error_code,
            message=exc.message,
            failure_class=FailureClass.TRANSIENT,
        )

    @classmethod
    def permanent(cls, exc: TaskError) -> "_AttemptOutcome":
        return cls(
            "failed",
            error_code=exc.error_code,
            message=exc.message,
            failure_class=FailureClass.PERMANENT,
        )

    @classmethod
    def timeout(cls, is_transient: bool) -> "_AttemptOutcome":
        return cls(
            "failed",
            error_code=ErrorCode.TIMEOUT,
            message="execution timed out",
            failure_class=(
                FailureClass.TRANSIENT if is_transient else FailureClass.PERMANENT
            ),
        )

    def as_permanent_after_side_effect(self) -> "_AttemptOutcome":
        """副作用已发生后，瞬时失败被强制升级为永久失败（防重复副作用）。"""
        return _AttemptOutcome(
            "failed",
            error_code=ErrorCode.PERMANENT_FAILURE,
            message=(
                "failure after side effect already committed; not retrying to "
                f"avoid duplicate side effect: {self.message}"
            ),
            failure_class=FailureClass.PERMANENT,
        )


# 任务在执行器内部所处的通道
_READY = "ready"
_WAITING = "waiting"
_ACTIVE = "active"  # 运行中或退避中（由 worker 管理）


class TaskExecutor:
    def __init__(
        self,
        store: TaskStore,
        handlers: HandlerRegistry,
        settings: Settings,
    ) -> None:
        self._store = store
        self._handlers = handlers
        self._settings = settings
        self._condition = asyncio.Condition()
        self._slots_taken = 0
        self._ready: deque[str] = deque()
        self._waiting: deque[tuple[int, str]] = deque()
        self._location: dict[str, str] = {}
        self._next_seq = 0
        self.stats = ConcurrencyStats()
        self._shutdown = False
        self._dispatch_task: asyncio.Task[None] | None = None
        self._worker_tasks: set[asyncio.Task[None]] = set()
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._terminal_events: dict[str, asyncio.Event] = {}

    # ── 生命周期 ─────────────────────────────────────────────────
    def start(self) -> None:
        if self._dispatch_task is None and not self._shutdown:
            self._dispatch_task = asyncio.create_task(self._dispatch_loop())

    async def shutdown(self, wait: bool = True) -> None:
        self._shutdown = True
        async with self._condition:
            for event in self._cancel_events.values():
                event.set()
            self._condition.notify_all()
        if self._dispatch_task is not None:
            self._dispatch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dispatch_task
            self._dispatch_task = None
        if wait:
            while self._worker_tasks:
                await asyncio.gather(*list(self._worker_tasks), return_exceptions=True)

    # ── 提交 ─────────────────────────────────────────────────────
    async def submit(self, record: TaskRecord) -> int:
        """同步裁决入队。返回排队位置（立即预留槽位时为 0，表示无需排队）。"""
        self.start()
        async with self._condition:
            if self._shutdown:
                self.stats.rejected += 1
                raise QueueFull("executor is shutting down")
            self._next_seq += 1
            seq = self._next_seq
            self._cancel_events.setdefault(record.task_id, asyncio.Event())
            self._terminal_events.setdefault(record.task_id, asyncio.Event())
            self.stats.submitted += 1

            if self._slots_taken < self._settings.max_concurrency:
                # 立即预留槽位
                self._slots_taken += 1
                self._ready.append(record.task_id)
                self._location[record.task_id] = _READY
                position = 0
            elif len(self._waiting) < self._settings.queue_capacity:
                self._waiting.append((seq, record.task_id))
                self._location[record.task_id] = _WAITING
                position = len(self._waiting)
            else:
                self.stats.rejected += 1
                raise QueueFull(
                    f"queue full (capacity={self._settings.queue_capacity}, "
                    f"max_concurrency={self._settings.max_concurrency})"
                )
            self._sync_stats_locked()
            self._condition.notify_all()

        await self._store.assign_enqueue_seq(record.task_id)
        await self._store.transition(
            record.task_id,
            TaskStatus.QUEUED,
            reason=(
                "slot reserved, awaiting worker"
                if position == 0
                else f"enqueued at waiting position {position}"
            ),
        )
        self._log_transition(record.task_id, TaskStatus.QUEUED, "enqueued")
        return position

    def queued_position(self, task_id: str) -> int | None:
        """等待队列中的位置（1 起）；已预留槽位/运行中返回 None。"""
        for index, (_, queued_id) in enumerate(self._waiting, start=1):
            if queued_id == task_id:
                return index
        return None

    def request_cancel(self, task_id: str) -> None:
        event = self._cancel_events.get(task_id)
        if event is not None:
            event.set()

    def is_cancel_requested(self, task_id: str) -> bool:
        event = self._cancel_events.get(task_id)
        return event is not None and event.is_set()

    async def wait_terminal(self, task_id: str, timeout: float = 1.0) -> bool:
        event = self._terminal_events.get(task_id)
        if event is None:
            return True
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=timeout)
        return event.is_set()

    async def cancel_if_queued(self, task_id: str) -> bool:
        """取消尚在等待/待启动的任务。

        返回 True 表示执行器已把它从队列移除（调用方随后终结状态）；
        返回 False 表示任务已在运行/退避或已终态，取消仅以信号方式协作生效。
        """
        async with self._condition:
            location = self._location.get(task_id)
            if location == _WAITING:
                self._waiting = deque(
                    item for item in self._waiting if item[1] != task_id
                )
                self._location.pop(task_id, None)
                self.request_cancel(task_id)
                self._sync_stats_locked()
                self._condition.notify_all()
                self._signal_terminal(task_id)
                self.stats.completed += 1
                return True
            if location == _READY:
                try:
                    self._ready.remove(task_id)
                except ValueError:
                    pass
                self._location.pop(task_id, None)
                self.request_cancel(task_id)
                # 让出预留槽位并提升一个等待者
                self._release_slot_and_promote_locked()
                self._sync_stats_locked()
                self._condition.notify_all()
                self._signal_terminal(task_id)
                self.stats.completed += 1
                return True
            # 已在 worker 中：仅置取消信号
            self.request_cancel(task_id)
            return False

    # ── 调度循环：只负责把 ready 中的任务交给 worker（槽位已预留） ──
    async def _dispatch_loop(self) -> None:
        while True:
            async with self._condition:
                while not self._ready and not self._shutdown:
                    await self._condition.wait()
                if self._shutdown and not self._ready:
                    return
                task_id = self._ready.popleft()
                self._location[task_id] = _ACTIVE
            worker = asyncio.create_task(self._run_worker(task_id))
            self._worker_tasks.add(worker)
            worker.add_done_callback(self._worker_tasks.discard)

    def _release_slot_and_promote_locked(self) -> None:
        """一个任务终止，释放槽位；若有等待者则把队首提升到 ready。"""
        self._slots_taken -= 1
        if self._waiting:
            _, promoted_id = self._waiting.popleft()
            self._ready.append(promoted_id)
            self._location[promoted_id] = _READY
            # 提升者消费刚释放的槽位：净槽位占用不变
            self._slots_taken += 1

    # ── worker ───────────────────────────────────────────────────
    async def _run_worker(self, task_id: str) -> None:
        self.stats.running += 1
        self.stats.max_running_seen = max(
            self.stats.max_running_seen, self.stats.running
        )
        terminal = False
        try:
            terminal = await self._run_with_retry(task_id)
        finally:
            self.stats.running -= 1
            async with self._condition:
                if terminal:
                    # 终态：释放本任务持有的槽位并按 FIFO 提升一个等待者
                    self._location.pop(task_id, None)
                    self._release_slot_and_promote_locked()
                else:
                    # 重试：槽位保留，任务已在 ready 中等待再次拉起
                    pass
                self._sync_stats_locked()
                self._condition.notify_all()
            if terminal:
                self.stats.completed += 1
                self._signal_terminal(task_id)
                self._cancel_events.pop(task_id, None)

    async def _run_with_retry(self, task_id: str) -> bool:
        """返回 True 表示到达终态；False 表示已转入退避后的重新等待。"""
        record = await self._store.get(task_id)
        assert record is not None
        deadline = self._compute_deadline(record)

        record = await self._store.mark_attempt_started(task_id)
        self._log_transition(task_id, TaskStatus.RUNNING, f"attempt {record.attempt}")
        outcome = await self._execute_attempt(record, deadline)

        if outcome.kind == "success":
            await self._store.mark_succeeded(task_id, outcome.result)
            self._log_transition(task_id, TaskStatus.SUCCEEDED, "succeeded")
            return True

        if outcome.kind == "cancelled":
            await self._store.transition(
                task_id,
                TaskStatus.CANCELLED,
                reason=outcome.message,
                error_code=ErrorCode.CANCELLED,
                failure_class=FailureClass.NONE,
            )
            self._log_transition(task_id, TaskStatus.CANCELLED, outcome.message)
            return True

        record = await self._store.get(task_id)
        assert record is not None
        # 副作用已经发生：后续失败一律按永久失败处理，禁止重试造成重复副作用
        if record.side_effect_occurred:
            outcome = outcome.as_permanent_after_side_effect()

        if outcome.failure_class == FailureClass.PERMANENT:
            await self._store.mark_failed(
                task_id, outcome.error_code, outcome.message, FailureClass.PERMANENT
            )
            self._log_transition(
                task_id, TaskStatus.FAILED, outcome.message, outcome.error_code.value
            )
            return True

        if record.attempt >= record.max_attempts:
            await self._store.mark_failed(
                task_id,
                ErrorCode.RETRIES_EXHAUSTED,
                f"transient failure after {record.attempt} attempt(s): {outcome.message}",
                FailureClass.TRANSIENT,
            )
            self._log_transition(
                task_id, TaskStatus.FAILED, "retries exhausted",
                ErrorCode.RETRIES_EXHAUSTED.value,
            )
            return True

        if deadline is not None and utcnow() >= deadline:
            await self._store.mark_failed(
                task_id,
                ErrorCode.TIMEOUT,
                "total execution deadline exceeded",
                FailureClass.TRANSIENT,
            )
            self._log_transition(
                task_id, TaskStatus.FAILED, "total deadline exceeded",
                ErrorCode.TIMEOUT.value,
            )
            return True

        await self._store.mark_retrying(task_id, outcome.message)
        self._log_transition(
            task_id, TaskStatus.RETRYING, outcome.message,
            ErrorCode.TRANSIENT_FAILURE.value,
        )
        # 退避期间 worker 继续持有该任务预留的槽位（不与 waiting 队列交互，
        # 槽位收支始终在 worker 入口/终态配对，杜绝泄漏或超卖）。
        backoff = self._backoff_seconds(record.attempt)
        if await self._sleep_with_cancel(task_id, backoff):
            await self._store.transition(
                task_id,
                TaskStatus.CANCELLED,
                reason="cancelled during retry backoff",
                error_code=ErrorCode.CANCELLED,
                failure_class=FailureClass.NONE,
            )
            self._log_transition(task_id, TaskStatus.CANCELLED, "cancelled in backoff")
            return True

        # 退避结束：槽位仍属于本任务，直接重新进入 ready 等待下一次拉起
        async with self._condition:
            if self._shutdown or self.is_cancel_requested(task_id):
                await self._store.transition(
                    task_id,
                    TaskStatus.CANCELLED,
                    reason="cancelled after retry backoff",
                    error_code=ErrorCode.CANCELLED,
                    failure_class=FailureClass.NONE,
                )
                return True
            self._ready.append(task_id)
            self._location[task_id] = _READY
            self._condition.notify_all()
        await self._store.transition(
            task_id, TaskStatus.QUEUED, reason="re-enqueued after backoff"
        )
        self._log_transition(task_id, TaskStatus.QUEUED, "re-enqueued")
        # worker 退出并保留槽位；dispatch 会再次拉起该任务（同一槽位）
        return False

    async def _execute_attempt(
        self, record: TaskRecord, deadline: Any
    ) -> _AttemptOutcome:
        task_id = record.task_id
        timeout = record.timeout_seconds
        handler = self._handlers.get(record.task_type)

        def _is_cancelled() -> bool:
            return self.is_cancel_requested(task_id)

        context = HandlerContext(
            task_id=task_id,
            idempotency_key=record.idempotency_key,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            trace_id=record.trace_id,
            request_id=record.request_id,
            side_effect=lambda: self._schedule_side_effect(task_id),
            is_cancelled=_is_cancelled,
        )
        coro = handler(record.payload, context)
        try:
            if timeout is not None:
                result = await asyncio.wait_for(coro, timeout=timeout)
            else:
                result = await coro
        except asyncio.TimeoutError:
            return _AttemptOutcome.timeout(self._settings.timeout_is_transient)
        except asyncio.CancelledError:
            return _AttemptOutcome.cancelled("cancelled while running")
        except PermanentTaskError as exc:
            return _AttemptOutcome.permanent(exc)
        except TransientTaskError as exc:
            return _AttemptOutcome.transient(exc)
        except TaskError as exc:
            if isinstance(exc, TaskTimeout):
                return _AttemptOutcome.timeout(self._settings.timeout_is_transient)
            return _AttemptOutcome.permanent(exc)
        except Exception as exc:
            # 已请求协作式取消时，处理器即便抛出未分类异常也归一为 CANCELLED，
            # 保证"取消 -> 终态"的结果确定、可解释，而不是落入失败/重试。
            if self.is_cancel_requested(task_id):
                return _AttemptOutcome.cancelled(
                    f"cancelled while running (handler raised {type(exc).__name__})"
                )
            if self._settings.unknown_error_is_transient:
                return _AttemptOutcome.transient(
                    TransientTaskError(f"{type(exc).__name__}: {exc}")
                )
            return _AttemptOutcome.permanent(
                PermanentTaskError(f"{type(exc).__name__}: {exc}")
            )
        else:
            return _AttemptOutcome.success(result)
        finally:
            coro.close()

    def _schedule_side_effect(self, task_id: str) -> None:
        asyncio.create_task(self._store.mark_side_effect(task_id))

    # ── 工具 ─────────────────────────────────────────────────────
    def _compute_deadline(self, record: TaskRecord) -> Any:
        total = self._settings.total_timeout_seconds
        if total is None:
            return None
        return record.created_at + timedelta(seconds=total)

    def _backoff_seconds(self, attempt: int) -> float:
        base = self._settings.retry_backoff_base_seconds
        cap = self._settings.retry_backoff_max_seconds
        return min(cap, base * (2 ** (attempt - 1)))

    async def _sleep_with_cancel(self, task_id: str, seconds: float) -> bool:
        """退避等待；返回 True 表示期间被取消。"""
        event = self._cancel_events.get(task_id)
        if event is None:
            await asyncio.sleep(seconds)
            return False
        if seconds <= 0:
            return event.is_set()
        try:
            await asyncio.wait_for(event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return False
        return True

    def _signal_terminal(self, task_id: str) -> None:
        event = self._terminal_events.get(task_id)
        if event is not None:
            event.set()

    def _sync_stats_locked(self) -> None:
        self.stats.slots_reserved = self._slots_taken
        self.stats.waiting = len(self._waiting)

    def _log_transition(
        self, task_id: str, to_status: TaskStatus, reason: str, error_code: str = ""
    ) -> None:
        request_id = trace_id = "-"
        attempt = 0
        record = self._store.peek(task_id)
        if record is not None:
            request_id = record.request_id
            trace_id = record.trace_id
            attempt = record.attempt
        log_transition(
            logger,
            from_status="*",
            to_status=to_status.value,
            request_id=request_id,
            trace_id=trace_id,
            task_id=task_id,
            attempt=attempt,
            running=self.stats.running,
            queued=self.stats.waiting,
            max_concurrency=self._settings.max_concurrency,
            queue_capacity=self._settings.queue_capacity,
            reason=reason,
            error_code=error_code,
        )
