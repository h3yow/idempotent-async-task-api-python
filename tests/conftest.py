"""pytest 公共夹具：小容量配置 + 日志打印请求标识、并发度与状态流转。"""

from __future__ import annotations

import logging

import pytest

from app.api import create_app
from app.config import Settings
from app.handlers import HandlerRegistry
from app.wiring import build_service

from .handlers_helpers import EffectLog, install_test_handlers

logger = logging.getLogger("task_tests")


def test_logger(message: str) -> None:
    """单测日志：统一前缀，便于在 -s 输出中观察请求标识/并发度/状态流转。"""
    logger.info("    | %s", message)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        max_concurrency=2,
        max_queue_size=2,
        task_timeout_seconds=1.0,
        max_retries=2,
        retry_backoff_seconds=0.0,
    )


@pytest.fixture
async def components(settings):
    """返回一个轻量容器，含 service/executor/store/effects。"""
    effects = EffectLog()
    handlers = HandlerRegistry()
    install_test_handlers(handlers, effects)
    service, executor, store = build_service(
        settings, handlers=handlers, log=test_logger)
    await executor.start()
    try:
        from types import SimpleNamespace

        bundle = SimpleNamespace(
            service=service,
            executor=executor,
            store=store,
            effects=effects,
            submit=service.submit,
            get=service.get,
            wait=service.wait_until_terminal,
            cancel=service.cancel,
        )
        yield bundle
    finally:
        await executor.stop()


@pytest.fixture
async def httpx_client(components):
    """ASGI 传输客户端，直连 FastAPI（不占网络端口）。"""
    from httpx import ASGITransport, AsyncClient

    fastapi_app = create_app(components.service)

    @pytest.fixture
    async def _lifecycle():  # 执行器已在 components 夹具中启动
        yield

    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
