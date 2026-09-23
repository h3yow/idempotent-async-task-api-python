"""内存存储：幂等键映射、FIFO 等待队列、并发额度计数。

所有对共享状态的读写均在一把异步锁内完成，保证：
- 不产生重复任务；
- 不超卖并发额度；
- 排队顺序确定（FIFO）。
"""

from __future__ import annotations

import asyncio
from collections import deque

from .config import Settings
from .models import TaskRecord


class TaskStore:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._tasks: dict[str, TaskRecord] = {}
        self._by_idempotency_key: dict[str, str] = {}
        self._queue: deque[str] = deque()  # 等待中的任务 id，FIFO
        self._running: set[str] = set()
        self._lock = asyncio.Lock()

    @property
    def lock(self) -> asyncio.Lock:
        """所有容量/队列/映射变更必须持有的锁。"""
        return self._lock

    def get_by_key(self, idempotency_key: str) -> TaskRecord | None:
        return self._tasks.get(self._by_idempotency_key.get(idempotency_key, ""))

    def get(self, task_id: str) -> TaskRecord | None:
        return self._tasks.get(task_id)

    def put(self, record: TaskRecord) -> None:
        """登记新任务（调用方需先确认幂等键不存在，且持锁）。"""
        if record.idempotency_key in self._by_idempotency_key:
            raise RuntimeError(f"幂等键已存在: {record.idempotency_key}")
        self._tasks[record.id] = record
        self._by_idempotency_key[record.idempotency_key] = record.id

    def queue_size(self) -> int:
        return len(self._queue)

    def running_count(self) -> int:
        return len(self._running)

    def has_capacity(self) -> bool:
        """是否还有并发槽位（运行数 < 上限）。"""
        return len(self._running) < self._settings.max_concurrency

    def can_enqueue(self) -> bool:
        """槽位已满但等待队列未满时可排队。"""
        return len(self._queue) < self._settings.max_queue_size

    def enqueue(self, task_id: str) -> None:
        self._queue.append(task_id)

    def dequeue_one(self) -> str | None:
        """按 FIFO 取出一个任务 id（仅取 id，占槽由 mark_running 完成）。

        若队首任务已不在 PENDING（例如排队期间被取消），跳过并清理。
        调用方需持有锁并已确认有并发容量。
        """
        while self._queue:
            task_id = self._queue.popleft()
            record = self._tasks.get(task_id)
            if record is not None and record.state.value == "PENDING":
                return task_id
        return None

    def mark_running(self, task_id: str) -> None:
        """占用一个并发槽位；超卖时立即报错（防御性不变量）。"""
        if task_id in self._running:
            raise RuntimeError(f"任务已在运行中: {task_id}")
        if len(self._running) >= self._settings.max_concurrency:
            raise RuntimeError(f"并发额度超卖: running={len(self._running)}")
        self._running.add(task_id)

    def mark_finished(self, task_id: str) -> None:
        """任务到达终态后释放并发槽位。"""
        self._running.discard(task_id)

    def remove_from_queue(self, task_id: str) -> bool:
        """从等待队列移除（排队中取消）；不在队列返回 False。"""
        try:
            self._queue.remove(task_id)
            return True
        except ValueError:
            return False

    def queued_position(self, task_id: str) -> int | None:
        """返回任务在队列中的位置（0 为首），不在队列返回 None。"""
        try:
            return self._queue.index(task_id)
        except ValueError:
            return None

    def all_records(self) -> list[TaskRecord]:
        return list(self._tasks.values())
