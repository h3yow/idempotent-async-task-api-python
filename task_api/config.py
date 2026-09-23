"""并发、队列、重试与错误分类的配置项。

可通过环境变量覆盖（前缀 ``TASK_API_``），例如::

    TASK_API_MAX_CONCURRENCY=4 TASK_API_QUEUE_CAPACITY=16 uvicorn main:app
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # ── 并发与背压 ────────────────────────────────────────────────
    #: 并发上限：同时处于 RUNNING 的任务数绝不超过该值
    max_concurrency: int = 2
    #: 有界等待队列容量；容量满后的新提交以 QUEUE_FULL 被拒绝
    queue_capacity: int = 4

    # ── 重试 ─────────────────────────────────────────────────────
    #: 默认最大尝试次数（1 = 不重试，2 = 首试 + 1 次重试）
    default_max_attempts: int = 3
    #: 瞬时失败重试退避基准（秒）：backoff = min(max, base * 2**(attempt-1))
    retry_backoff_base_seconds: float = 0.05
    #: 重试退避上限（秒）
    retry_backoff_max_seconds: float = 2.0

    # ── 超时 ─────────────────────────────────────────────────────
    #: 单次执行默认超时（秒）；None 表示不限制
    default_timeout_seconds: float | None = 5.0
    #: 整体执行（含全部重试）截止时间（秒）；None 表示不限制
    total_timeout_seconds: float | None = None

    # ── 错误分类 ─────────────────────────────────────────────────
    #: 超时归类为瞬时失败（可重试）还是永久失败
    timeout_is_transient: bool = True
    #: 处理器抛出的、未显式分类的异常是否视为瞬时失败
    unknown_error_is_transient: bool = True


def _env(name: str, default):
    """读取环境变量；空字符串视为未设置。"""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw


def _env_int(name: str, default: int) -> int:
    raw = _env(name, None)
    return int(raw) if raw is not None else default


def _env_float(name: str, default: float | None) -> float | None:
    raw = _env(name, None)
    if raw is None:
        return default
    return float(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, None)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def get_settings() -> Settings:
    """从环境变量装配 :class:`Settings`。"""
    return Settings(
        max_concurrency=_env_int("TASK_API_MAX_CONCURRENCY", Settings.max_concurrency),
        queue_capacity=_env_int("TASK_API_QUEUE_CAPACITY", Settings.queue_capacity),
        default_max_attempts=_env_int(
            "TASK_API_DEFAULT_MAX_ATTEMPTS", Settings.default_max_attempts
        ),
        retry_backoff_base_seconds=_env_float(
            "TASK_API_RETRY_BACKOFF_BASE_SECONDS",
            Settings.retry_backoff_base_seconds,
        ),
        retry_backoff_max_seconds=_env_float(
            "TASK_API_RETRY_BACKOFF_MAX_SECONDS",
            Settings.retry_backoff_max_seconds,
        ),
        default_timeout_seconds=_env_float(
            "TASK_API_DEFAULT_TIMEOUT_SECONDS", Settings.default_timeout_seconds
        ),
        total_timeout_seconds=_env_float(
            "TASK_API_TOTAL_TIMEOUT_SECONDS", Settings.total_timeout_seconds
        ),
        timeout_is_transient=_env_bool(
            "TASK_API_TIMEOUT_IS_TRANSIENT", Settings.timeout_is_transient
        ),
        unknown_error_is_transient=_env_bool(
            "TASK_API_UNKNOWN_ERROR_IS_TRANSIENT",
            Settings.unknown_error_is_transient,
        ),
    )
