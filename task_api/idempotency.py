"""幂等控制：请求指纹。

同一 ``idempotency_key`` 的请求会在服务端留存首次提交的业务内容指纹。
- 指纹相同（同键同内容）：返回既有任务，绝不产生重复任务/副作用；
- 指纹不同（同键异内容）：抛 :class:`~task_api.errors.IdempotencyConflict`，
  错误码 ``IDEMPOTENCY_KEY_CONFLICT`` 与"首次提交的正常复用"明确区分。

指纹只覆盖**业务内容**（task_type + payload），不包含 idempotency_key 自身，
也不包含 max_attempts / timeout_seconds 等非业务执行参数。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def compute_request_hash(task_type: str, payload: dict[str, Any]) -> str:
    """对请求业务内容计算稳定的 SHA-256 指纹。

    使用 ``sort_keys=True`` 的规范 JSON 序列化，保证键顺序不同但
    内容相同的请求得到相同指纹。
    """
    canonical = json.dumps(
        {"task_type": task_type, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
