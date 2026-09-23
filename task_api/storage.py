"""任务存储（内存实现）：协程安全的任务表与状态机守卫。

所有对 :class:`~task_api.models.TaskRecord` 的变更都在同一把
:class:`asyncio.Lock` 内完成，确保：

1. 幂等注册（key -> task）与任务创建是原子的，重复提交不会建出两条记录；
2. 每次状态流转都经过 :data:`~task_api.models.ALLOWED_TRANSITIONS` 校验，
   非法流转直接抛错，任务状态在超时/取消/重试之间始终一致；
3. 每次流转都追加一条带 request_id / trace_id / attempt 的
   :class:`~task_api.models.TransitionEvent`，链路可追溯。
"""

from __future__ import annotations

import asyncio

from task_api.errors import ErrorCode, TaskError
from task_api.models import (
    ALLOWED_TRANSITIONS,
    FailureClass,
    TaskRecord,
    TaskStatus,
    TransitionEvent,
    utcnow,
)
from task_api.tracing import get_request_id, get_trace_id


class InvalidStateTransition(TaskError):
    """内部不变量被破坏：非法的状态流转。"""

    def __init__(self, from_status: TaskStatus, to_status: TaskStatus) -> None:
        super().__init__(
            ErrorCode.INVALID_STATE_TRANSITION,
            f"illegal transition {from_status.value} -> {to_status.value}",
        )


class TaskStore:
    """内存任务存储。"""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._by_idempotency_key: dict[str, str] = {}
        self._lock = asyncio.Lock()

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    # ── 创建与查询 ───────────────────────────────────────────────
    async def create(self, record: TaskRecord) -> None:
        async with self._lock:
            if record.task_id in self._tasks:
                raise InvalidStateTransition(
                    TaskStatus.PENDING, TaskStatus.PENDING
                )
            if record.idempotency_key in self._by_idempotency_key:
                # 调用方必须先查重；走到这里说明幂等编排有缺陷
                raise InvalidStateTransition(
                    TaskStatus.PENDING, TaskStatus.PENDING
                )
            self._insert_unlocked(record)

    async def find_or_create(
        self, record: TaskRecord
    ) -> tuple[TaskRecord, bool]:
        """按幂等键原子地"查找或创建"。

        返回 ``(record, created)``：
        - 键已存在：返回既有记录与 ``False``（不新建、不产生重复任务）；
        - 键不存在：插入新记录并追加 PENDING 事件，返回 ``(record, True)``。

        查重与插入在同一把锁内完成，杜绝并发重复提交建出两条记录。
        """
        async with self._lock:
            existing_id = self._by_idempotency_key.get(record.idempotency_key)
            if existing_id is not None:
                return self._tasks[existing_id], False
            self._insert_unlocked(record)
            return record, True

    def _insert_unlocked(self, record: TaskRecord) -> None:
        self._tasks[record.task_id] = record
        self._by_idempotency_key[record.idempotency_key] = record.task_id
        self._append_event_unlocked(
            record,
            None,
            TaskStatus.PENDING,
            reason="task accepted",
        )

    async def get(self, task_id: str) -> TaskRecord | None:
        async with self._lock:
            return self._tasks.get(task_id)

    def peek(self, task_id: str) -> TaskRecord | None:
        """不加锁的只读快照（dict 读取为原子操作，仅用于日志字段）。"""
        return self._tasks.get(task_id)

    async def get_by_idempotency_key(self, key: str) -> TaskRecord | None:
        async with self._lock:
            task_id = self._by_idempotency_key.get(key)
            if task_id is None:
                return None
            return self._tasks[task_id]

    async def all(self) -> list[TaskRecord]:
        async with self._lock:
            return list(self._tasks.values())

    # ── 排队序号（在锁内分配，确定 FIFO 顺序） ────────────────────
    async def assign_enqueue_seq(self, task_id: str) -> int:
        async with self._lock:
            record = self._tasks[task_id]
            record.enqueue_seq += 1
            record.updated_at = utcnow()
            return record.enqueue_seq

    # ── 状态流转（状态机守卫） ───────────────────────────────────
    async def transition(
        self,
        task_id: str,
        to_status: TaskStatus,
        *,
        reason: str = "",
        error_code: ErrorCode | None = None,
        failure_class: FailureClass | None = None,
    ) -> TaskRecord:
        async with self._lock:
            return self._transition_unlocked(
                task_id,
                to_status,
                reason=reason,
                error_code=error_code,
                failure_class=failure_class,
            )

    def _transition_unlocked(
        self,
        task_id: str,
        to_status: TaskStatus,
        *,
        reason: str = "",
        error_code: ErrorCode | None = None,
        failure_class: FailureClass | None = None,
    ) -> TaskRecord:
        record = self._tasks[task_id]
        from_status = record.status
        if to_status not in ALLOWED_TRANSITIONS.get(from_status, frozenset()):
            raise InvalidStateTransition(from_status, to_status)

        record.status = to_status
        record.updated_at = utcnow()
        if error_code is not None:
            record.error_code = error_code
        if failure_class is not None:
            record.failure_class = failure_class
        if reason and error_code is not None:
            record.error_message = reason
        elif reason:
            record.error_message = record.error_message or reason

        if to_status == TaskStatus.RUNNING:
            record.started_at = record.started_at or utcnow()
        if to_status.is_terminal:
            record.finished_at = utcnow()

        self._append_event_unlocked(
            record, from_status, to_status, reason=reason, error_code=error_code
        )
        update_fields = {"task_id", "status"}
        if error_code is not None:
            update_fields.add("error_code")
        if failure_class is not None:
            update_fields.add("failure_class")
        return record

    def _append_event_unlocked(
        self,
        record: TaskRecord,
        from_status: TaskStatus | None,
        to_status: TaskStatus,
        *,
        request_id: str = "",
        trace_id: str = "",
        reason: str = "",
        error_code: ErrorCode | None = None,
    ) -> None:
        event = TransitionEvent(
            seq=len(record.history) + 1,
            from_status=from_status,
            to_status=to_status,
            at=utcnow(),
            attempt=record.attempt,
            # 优先显式传入，其次记录上下文，最后回退到记录上的稳定标识
            request_id=request_id or get_request_id() or record.request_id,
            trace_id=trace_id or get_trace_id() or record.trace_id,
            reason=reason,
            error_code=error_code,
        )
        # contextvars 在后台任务中可能为空，强制以任务记录上的标识为准
        if not request_id:
            event.request_id = record.request_id
        if not trace_id:
            event.trace_id = record.trace_id
        record.history.append(event)

    # ── 高层语义方法 ─────────────────────────────────────────────
    async def mark_attempt_started(self, task_id: str) -> TaskRecord:
        async with self._lock:
            record = self._tasks[task_id]
            record.attempt += 1
            return self._transition_unlocked(
                task_id,
                TaskStatus.RUNNING,
                reason=f"attempt {record.attempt} started",
            )

    async def mark_succeeded(self, task_id: str, result: object) -> TaskRecord:
        async with self._lock:
            record = self._tasks[task_id]
            record.result = result
            record.failure_class = FailureClass.NONE
            return self._transition_unlocked(
                task_id,
                TaskStatus.SUCCEEDED,
                reason="handler succeeded",
            )

    async def mark_failed(
        self,
        task_id: str,
        error_code: ErrorCode,
        message: str,
        failure_class: FailureClass,
    ) -> TaskRecord:
        async with self._lock:
            record = self._tasks[task_id]
            record.error_message = message
            record.error_code = error_code
            return self._transition_unlocked(
                task_id,
                TaskStatus.FAILED,
                reason=message,
                error_code=error_code,
                failure_class=failure_class,
            )

    async def mark_retrying(self, task_id: str, message: str) -> TaskRecord:
        async with self._lock:
            return self._transition_unlocked(
                task_id,
                TaskStatus.RETRYING,
                reason=message,
                error_code=ErrorCode.TRANSIENT_FAILURE,
                failure_class=FailureClass.TRANSIENT,
            )

    async def mark_side_effect(self, task_id: str) -> None:
        async with self._lock:
            self._tasks[task_id].side_effect_occurred = True
            self._tasks[task_id].updated_at = utcnow()
