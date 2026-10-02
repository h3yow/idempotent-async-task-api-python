# 幂等异步任务提交与执行 API（Python / FastAPI）

带幂等控制、并发限流（背压）、失败分类重试与**副作用保护**的异步任务服务。

- **幂等提交**：相同幂等键的重复提交复用同一任务、无重复副作用；同键不同内容返回
  `409 idempotency_content_conflict`。
- **并发上限**：RUNNING 任务数不超过 `max_concurrency`，超出按 FIFO 排队；
  队列也满时返回 `503 capacity_queue_full`，不超卖、不静默丢弃。
- **失败分类与重试**：瞬时失败（含超时）退避后重新排队重试，永久失败立即终结；
  每次尝试与状态流转均带 `request_id`、并发度与备注，全链路可追踪。
- **副作用保护**：处理器在不可重复的动作生效后调用 `commit_side_effect()`，
  此后任何失败/超时都不再重试，任务以 `ABORTED`（`side_effect_committed`）终结，
  与「什么都没发生就失败」明确区分；并发名额正常归还。
- **幂等键反查**：`GET /tasks/by-key/{key}` 凭幂等键查回状态、尝试次数、失败原因、
  `side_effect_committed` 与 `will_retry`；查无此键返回 `404 task_not_found`。
- **提交级覆盖**：可选 `max_retries`（0..10）与 `time_budget_seconds`（0.1..3600），
  不指定则沿用默认；预算用尽以 `budget_exhausted` 与单次超时区分；
  非法取值返回 `400 invalid_parameter`。

## 快速开始

```bash
uv sync --group dev
uv run pytest -q                      # 35 个单测
uv run pytest --log-cli-level=INFO    # 含请求标识/并发度/状态流转/副作用次数日志
uv run uvicorn main:app --port 8000   # 启动服务
```

## 深入阅读

状态机、副作用保护语义、幂等/重试规则、覆盖参数范围与默认值、幂等键反查、
并发与错误分类配置、接口与本地验证示例见 [docs/design.md](docs/design.md)。
