"""应用服务层：提交 / 查询 / 取消的幂等编排。

这是"不同入口得到一致结果"的核心：HTTP、测试或直接调用都经由本服务，
因此幂等判定、背压拒绝与状态流转规则只有一份权威实现。
"""

from __future__ import annotations

from task_api.config import Settings
from task_api.errors import (
    ErrorCode,
    IdempotencyConflict,
    QueueFull,
    TaskNotCancellable,
    TaskNotFound,
)
from task_api.executor import TaskExecutor
from task_api.handlers import HandlerRegistry
from task_api.idempotency import compute_request_hash
from task_api.logging_setup import get_logger
from task_api.models import (
    FailureClass,
    SubmitRequest,
    TaskRecord,
    TaskStatus,
    TaskView,
    CancelResponse,
)
from task_api.storage import TaskStore
from task_api.tracing import get_request_id, get_trace_id, new_id

logger = get_logger("service")

#: 允许发起取消的状态；终态任务的取消返回 TASK_ALREADY_TERMINAL 语义
_CANCELLABLE = frozenset(
    {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.RETRYING}
)


class TaskService:
    def __init__(
        self,
        store: TaskStore,
        executor: TaskExecutor,
        handlers: HandlerRegistry,
        settings: Settings,
    ) -> None:
        self._store = store
        self._executor = executor
        self._handlers = handlers
        self._settings = settings

    # ── 提交 ─────────────────────────────────────────────────────
    async def submit(
        self,
        request: SubmitRequest,
        *,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> TaskView:
        """幂等提交。

        - 同键 + 同内容：返回**既有任务**的当前视图（不重复创建、不重复执行）；
        - 同键 + 不同内容：抛 :class:`IdempotencyConflict`
          （``IDEMPOTENCY_KEY_CONFLICT``），与正常复用明确区分；
        - 队列已满：任务以 CANCELLED 终态留痕后抛 :class:`QueueFull`
          （``QUEUE_FULL``），不静默丢弃、不超卖。
        """
        rid = request_id or get_request_id()
        tid = trace_id or get_trace_id()
        request_hash = compute_request_hash(request.task_type, request.payload)

        task_id = new_id("task")
        record = TaskRecord(
            task_id=task_id,
            idempotency_key=request.idempotency_key,
            task_type=request.task_type,
            payload=request.payload,
            request_hash=request_hash,
            request_id=rid,
            trace_id=tid,
            max_attempts=request.max_attempts or self._settings.default_max_attempts,
            timeout_seconds=(
                request.timeout_seconds
                if request.timeout_seconds is not None
                else self._settings.default_timeout_seconds
            ),
        )
        # 查重 + 创建原子完成（存储层单锁），杜绝并发重复提交建出两条记录
        existing, created = await self._store.find_or_create(record)
        if not created:
            if existing.request_hash != request_hash:
                logger.warning(
                    "idempotency conflict key=%s existing_task=%s "
                    "existing_type=%s new_type=%s",
                    request.idempotency_key,
                    existing.task_id,
                    existing.task_type,
                    request.task_type,
                    extra={"task_id": existing.task_id},
                )
                raise IdempotencyConflict(
                    "idempotency key was already used with a different request "
                    f"(existing_task_id={existing.task_id}, "
                    f"existing_task_type={existing.task_type})"
                )
            # 同键同内容：幂等命中，返回既有任务
            logger.info(
                "idempotent replay key=%s task=%s status=%s",
                request.idempotency_key,
                existing.task_id,
                existing.status.value,
                extra={"task_id": existing.task_id},
            )
            return existing.to_view(
                queued_position=self._executor.queued_position(existing.task_id)
            )

        # 注册处理器校验放在建记录之后、入队之前：未注册类型直接失败留痕
        if request.task_type not in self._handlers:
            await self._store.mark_failed(
                task_id,
                ErrorCode.HANDLER_NOT_FOUND,
                f"handler not found for task_type={request.task_type!r}",
                FailureClass.PERMANENT,
            )
            from task_api.errors import HandlerNotFound

            raise HandlerNotFound(request.task_type)

        try:
            position = await self._executor.submit(record)
        except QueueFull:
            # 背压拒绝：任务终结为 CANCELLED 并留痕，错误原因可区分
            await self._store.transition(
                task_id,
                TaskStatus.CANCELLED,
                reason="rejected: queue full (backpressure)",
                error_code=ErrorCode.QUEUE_FULL,
                failure_class=FailureClass.NONE,
            )
            raise

        fresh = await self._store.get(task_id)
        assert fresh is not None
        return fresh.to_view(queued_position=position)

    # ── 查询 ─────────────────────────────────────────────────────
    async def get_task(self, task_id: str) -> TaskView:
        record = await self._store.get(task_id)
        if record is None:
            raise TaskNotFound(f"task_id={task_id!r}")
        return record.to_view(
            queued_position=self._executor.queued_position(task_id)
        )

    async def get_by_idempotency_key(self, key: str) -> TaskView:
        record = await self._store.get_by_idempotency_key(key)
        if record is None:
            raise TaskNotFound(f"idempotency_key={key!r}")
        return record.to_view(
            queued_position=self._executor.queued_position(record.task_id)
        )

    # ── 取消 ─────────────────────────────────────────────────────
    async def cancel(self, task_id: str, *, wait: bool = True) -> CancelResponse:
        """协作式取消。

        - QUEUED/RETRYING：通过执行器移除或唤醒，状态确定性地落到 CANCELLED；
        - RUNNING：发出取消信号，处理器应通过 ``ctx.is_cancelled()`` 协作退出；
        - 终态任务：不可取消，抛 :class:`TaskNotCancellable`。
        """
        record = await self._store.get(task_id)
        if record is None:
            raise TaskNotFound(f"task_id={task_id!r}")
        if record.status.is_terminal:
            raise TaskNotCancellable(
                f"task already terminal: {record.status.value}"
            )
        if record.status not in _CANCELLABLE:
            raise TaskNotCancellable(
                f"cannot cancel in status {record.status.value}"
            )

        # 1) 尚在等待/待启动：执行器同步移除，随后在状态机里确定性终结
        removed = await self._executor.cancel_if_queued(task_id)
        if removed:
            fresh = await self._store.get(task_id)
            if fresh is not None and fresh.status != TaskStatus.CANCELLED:
                await self._store.transition(
                    task_id,
                    TaskStatus.CANCELLED,
                    reason="cancelled while queued",
                    error_code=ErrorCode.CANCELLED,
                    failure_class=FailureClass.NONE,
                )
            self._executor.request_cancel(task_id)
            if wait:
                await self._executor.wait_terminal(task_id, timeout=2.0)
            final = await self._store.get(task_id)
            assert final is not None
            return CancelResponse(
                task_id=task_id,
                status=final.status,
                cancelled=final.status == TaskStatus.CANCELLED,
            )

        # 2) RUNNING/RETRYING：协作式取消（处理器应检查 ctx.is_cancelled()）
        self._executor.request_cancel(task_id)
        if wait:
            await self._executor.wait_terminal(task_id, timeout=2.0)

        fresh = await self._store.get(task_id)
        assert fresh is not None
        cancelled = fresh.status == TaskStatus.CANCELLED
        return CancelResponse(
            task_id=task_id, status=fresh.status, cancelled=cancelled
        )

    async def shutdown(self) -> None:
        await self._executor.shutdown(wait=True)
