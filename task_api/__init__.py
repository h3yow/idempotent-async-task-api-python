"""带幂等控制、并发限流与失败分类的异步任务 API。

ASGI 入口：
    - ``uvicorn main:app``
    - ``uvicorn task_api.app:app``
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
