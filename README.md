# 幂等异步任务提交与执行 API（Python / FastAPI）

带幂等控制、并发限流（背压）与失败分类重试的异步任务服务。

- **幂等提交**：相同幂等键的重复提交复用同一任务、无重复副作用；同键不同内容返回
  `409 idempotency_content_conflict`。
- **并发上限**：RUNNING 任务数不超过 `max_concurrency`，超出按 FIFO 排队；
  队列也满时返回 `503 capacity_queue_full`，不超卖、不静默丢弃。
- **失败分类与重试**：瞬时失败（含超时）退避后重新排队重试，永久失败立即终结；
  每次尝试与状态流转均带 `request_id`、并发度与备注，全链路可追踪。
- **副作用保护**：处理器在不可重复动作（扣款/通知/落单）生效后调用
  `ctx.mark_side_effect()`；此后无论遇到哪种失败（瞬时/永久/超时/预算用尽）
  都**绝不再重试**，任务统一以 `side_effect_committed` 原因终结
  （与普通失败、重试耗尽可区分），并发名额正常归还，动作保证只发生一次。
- **幂等键反查**：`GET /tasks/by-key/{key}` 凭幂等键找回任务状态、尝试次数、
  失败原因与副作用标记；纯读取不触发执行，查不到返回 `404 task_not_found`。
- **提交级覆盖**：可选 `max_retries`（0..10）与 `time_budget_seconds`（(0, 3600]），
  缺省完全沿用默认行为；整体预算是**硬上界**——执行中途到点立即打断，
  哪怕这次执行眼看就要成功，也以 `budget_exhausted` 终结（与单次超时
  `timeout` 明确区分）；非法取值返回 `422 invalid_request_parameter`。

## 快速开始

```bash
uv sync --group dev
uv run pytest -q                      # 45 个单测，默认实时输出
                                      # 请求标识/并发度/状态流转/副作用次数日志
uv run uvicorn main:app --port 8000   # 启动服务
```

## 深入阅读

状态机、幂等/重试规则、并发与错误分类配置、接口与验证示例见
[docs/design.md](docs/design.md)。
