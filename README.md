# 幂等异步任务提交与执行 API（Python / FastAPI）

带幂等控制、并发限流（背压）与失败分类重试的异步任务服务。

- **幂等提交**：相同幂等键的重复提交复用同一任务、无重复副作用；同键不同内容返回
  `409 idempotency_content_conflict`。
- **并发上限**：RUNNING 任务数不超过 `max_concurrency`，超出按 FIFO 排队；
  队列也满时返回 `503 capacity_queue_full`，不超卖、不静默丢弃。
- **失败分类与重试**：瞬时失败（含超时）退避后重新排队重试，永久失败立即终结；
  每次尝试与状态流转均带 `request_id`、并发度与备注，全链路可追踪。

## 快速开始

```bash
uv sync --group dev
uv run pytest -q                      # 17 个单测
uv run pytest --log-cli-level=INFO    # 含请求标识/并发度/状态流转日志
uv run uvicorn main:app --port 8000   # 启动服务
```

## 深入阅读

状态机、幂等/重试规则、并发与错误分类配置、接口与验证示例见
[docs/design.md](docs/design.md)。
