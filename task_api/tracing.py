"""请求标识（request_id）与链路追踪标识（trace_id）。

两者通过 :mod:`contextvars` 在同一次提交/执行的异步调用链中自动传播，
并被写入每条 :class:`~task_api.models.TransitionEvent` 与日志行，
使任务的完整执行链路可关联、可解释。
"""

from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Any

_request_id_var: ContextVar[str] = ContextVar("request_id", default="")
_trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")


def new_id(prefix: str) -> str:
    """生成带前缀的短标识，例如 ``task_3f25c...``。"""
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _clean_header(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()[:128]


def get_request_id() -> str:
    """当前上下文的请求标识；未设置时自动生成一个。"""
    value = _request_id_var.get()
    if not value:
        value = new_id("req")
        _request_id_var.set(value)
    return value


def set_request_id(request_id: str) -> Any:
    """设置请求标识，返回 token 供 reset 使用。"""
    return _request_id_var.set(request_id)


def get_trace_id() -> str:
    """当前上下文的链路标识；未设置时自动生成一个。"""
    value = _trace_id_var.get()
    if not value:
        value = new_id("trace")
        _trace_id_var.set(value)
    return value


def set_trace_id(trace_id: str) -> Any:
    """设置链路标识，返回 token 供 reset 使用。"""
    return _trace_id_var.set(trace_id)


def resolve_request_id(header_value: str | None = None) -> str:
    """解析入口请求标识：优先使用客户端传入的 X-Request-ID，否则生成。"""
    cleaned = _clean_header(header_value)
    if cleaned:
        set_request_id(cleaned)
        return cleaned
    return get_request_id()


def resolve_trace_id(
    header_value: str | None = None, *, request_id: str | None = None
) -> str:
    """解析链路标识：优先 X-Trace-ID；缺省时与 request_id 关联（同前缀派生）。"""
    cleaned = _clean_header(header_value)
    if cleaned:
        set_trace_id(cleaned)
        return cleaned
    value = get_trace_id()
    if not value and request_id:
        value = f"trace_of_{request_id}"
        set_trace_id(value)
    return value
