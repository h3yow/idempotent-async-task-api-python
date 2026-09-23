"""任务服务：把幂等控制、容量判定与执行调度编排在一起。"""

from __future__ import annotations

from .config import Settings
from .errors import (
    IdempotencyConflictError,
    QueueFullError,
    TaskNotFoundError,
)
from .executor import TaskExecutor
from .models import SubmitRequest, TaskRecord, new_task_id
from .store import TaskStore


class TaskService:
    def __init__(
        self,
        store: TaskStore,
        executor: TaskExecutor,
        settings: Settings,
    ) -> None:
        self._store = store
        self._executor = executor
        self._settings = settings

    async def submit(self, request: SubmitRequest) -> tuple[TaskRecord, bool, str]:
        """提交任务。

        返回 (记录, 是否新建, 受理原因)，受理原因：
        - created_running：首次提交且有空闲槽位，立即执行；
        - created_queued：首次提交但槽位已满，进入 FIFO 队列；
        - duplicate_replay：幂等键重复且内容一致，复用既有记录。

        异常：
        - IdempotencyConflictError：相同幂等键携带不同请求内容；
        - QueueFullError：槽位与队列均满，拒绝（不静默丢弃）。
        """
        fingerprint = request.content_fingerprint()
        async with self._store.lock:
            existing = self._store.get_by_key(request.idempotency_key)
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    raise IdempotencyConflictError(
                        f"幂等键 {request.idempotency_key!r} 已绑定不同的请求内容 "
                        f"(既有指纹 {existing.fingerprint[:12]}..., "
                        f"本次指纹 {fingerprint[:12]}...)"
                    )
                # 重复提交：无论任务处于何种状态，都复用同一记录，绝不重复执行
                return existing, False, "duplicate_replay"

            return await self._accept_new(request, fingerprint)

    async def _accept_new(
        self, request: SubmitRequest, fingerprint: str
    ) -> tuple[TaskRecord, bool, str]:
        """调用方必须持有 store.lock。容量判定先于登记，被拒请求不占用幂等键。"""
        if self._store.has_capacity():
            reason = "created_running"
        elif self._store.can_enqueue():
            reason = "created_queued"
        else:
            raise QueueFullError(
                f"系统容量已满：running={self._store.running_count()}/"
                f"{self._settings.max_concurrency}, "
                f"queue={self._store.queue_size()}/{self._settings.max_queue_size}"
            )

        record = TaskRecord(
            id=new_task_id(),
            idempotency_key=request.idempotency_key,
            task_type=request.task_type,
            payload=dict(request.payload),
            request_id=request.request_id or "req-" + new_task_id()[5:12],
            fingerprint=fingerprint,
            max_retries=self._settings.max_retries,
        )
        record.history.append(
            (record.created_at, record.request_id, record.state, record.state, "accepted")
        )
        self._store.put(record)

        if reason == "created_running":
            # 容量判定与占用在同一临界区内，杜绝超卖
            self._store.mark_running(record.id)
            self._executor.spawn_direct(record.id)
        else:
            self._store.enqueue(record.id)
            self._executor.notify()
        return record, True, reason

    async def get(self, task_id: str) -> TaskRecord:
        record = self._store.get(task_id)
        if record is None:
            raise TaskNotFoundError(f"任务不存在: {task_id}")
        return record

    async def cancel(self, task_id: str) -> TaskRecord:
        if self._store.get(task_id) is None:
            raise TaskNotFoundError(f"任务不存在: {task_id}")
        return await self._executor.cancel(task_id)

    async def wait_until_terminal(
        self, task_id: str, timeout: float | None = None
    ) -> TaskRecord:
        if self._store.get(task_id) is None:
            raise TaskNotFoundError(f"任务不存在: {task_id}")
        return await self._executor.wait_until_terminal(task_id, timeout)
