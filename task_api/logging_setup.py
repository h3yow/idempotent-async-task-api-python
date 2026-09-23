"""统一日志：时间、级别、请求标识、链路标识、任务标识、并发度与状态流转。

单测通过 ``caplog`` / 显式 ``logger`` 即可看到形如::

    2026-09-23 ... INFO [req_.. trace_.. task_..] running=1/2 queued=2/4 \
        transition QUEUED->RUNNING attempt=1
"""

from __future__ import annotations

import logging

_DEFAULT_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s "
    "[req_id=%(request_id)s trace_id=%(trace_id)s task_id=%(task_id)s] "
    "concurrency(running=%(running)d/%(max_concurrency)d queued=%(queued)d/%(queue_capacity)d) "
    "%(message)s"
)


class _ContextFilter(logging.Filter):
    """为每条日志补齐上下文字段，缺失时使用占位符。"""

    def __init__(self, settings_provider=None) -> None:
        super().__init__()
        self._settings_provider = settings_provider

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        if not hasattr(record, "request_id"):
            record.request_id = "-"
        if not hasattr(record, "trace_id"):
            record.trace_id = "-"
        if not hasattr(record, "task_id"):
            record.task_id = "-"
        if not hasattr(record, "running"):
            record.running = 0
        if not hasattr(record, "queued"):
            record.queued = 0
        if self._settings_provider is not None:
            settings = self._settings_provider()
            record.max_concurrency = settings.max_concurrency
            record.queue_capacity = settings.queue_capacity
        else:
            record.max_concurrency = getattr(record, "max_concurrency", -1)
            record.queue_capacity = getattr(record, "queue_capacity", -1)
        return True


_configured = False


def configure_logging(
    level: int = logging.INFO,
    *,
    settings_provider=None,
    force: bool = False,
) -> None:
    """配置根日志（幂等：默认只配置一次，``force`` 可重置）。"""
    global _configured
    if _configured and not force:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
    handler.addFilter(_ContextFilter(settings_provider))
    root = logging.getLogger("task_api")
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
    root.propagate = False
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """获取 task_api 命名空间下的 logger。"""
    if not name.startswith("task_api"):
        name = f"task_api.{name}"
    return logging.getLogger(name)


def log_transition(
    logger: logging.Logger,
    *,
    from_status: str | None,
    to_status: str,
    request_id: str = "-",
    trace_id: str = "-",
    task_id: str = "-",
    attempt: int = 0,
    running: int = 0,
    queued: int = 0,
    max_concurrency: int = -1,
    queue_capacity: int = -1,
    reason: str = "",
    error_code: str = "",
) -> None:
    """打印一条结构化的状态流转日志。"""
    msg = f"transition {from_status}->{to_status} attempt={attempt}"
    if reason:
        msg += f" reason={reason}"
    if error_code:
        msg += f" error_code={error_code}"
    logger.info(
        msg,
        extra={
            "request_id": request_id,
            "trace_id": trace_id,
            "task_id": task_id,
            "running": running,
            "queued": queued,
            "max_concurrency": max_concurrency,
            "queue_capacity": queue_capacity,
        },
    )
