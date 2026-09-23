# 幂等异步任务 API（Python / FastAPI）

一套带**幂等控制**、**并发限流与背压**、**失败分类与重试**、**链路追踪**的
异步任务提交与执行接口。无论是 HTTP 入口还是直接调用服务层，重复提交、
并发过载、超时/取消/重试都得到**一致且可解释**的结果。

## 1. 能力总览

| 主题 | 保证 |
|------|------|
| 幂等提交 | 相同 `idempotency_key` + 相同请求内容：复用同一任务，不产生重复任务/副作用 |
| 冲突区分 | 相同 key + **不同**请求内容：明确拒绝，错误码 `IDEMPOTENCY_KEY_CONFLICT`（HTTP 409） |
| 并发上限 | 同时 `RUNNING` 的任务数绝不超过 `max_concurrency`（槽位预留，绝不超卖） |
| 背压 | 超出并发容量进入有界 FIFO 等待队列；队列满以 `QUEUE_FULL` 拒绝（HTTP 503），不静默丢弃 |
| 排队顺序 | 等待队列严格 FIFO，顺序由提交序号确定；拒绝只可能发生在"并发满 + 队列满" |
| 失败分类 | 瞬时失败（可重试）与永久失败（不重试）明确区分 |
| 重试安全 | 指数退避重试；一旦副作用已提交（`ctx.side_effect()`），后续失败强制永久化，杜绝重复副作用 |
| 取消 | QUEUED/RETRYING 确定性取消；RUNNING 协作式取消，统一落到 `CANCELLED` |
| 追踪 | 每个任务携带 `request_id` / `trace_id`，每次状态流转都入历史并打日志 |

## 2. 任务状态机

```
                 ┌──────────────────────────(受理)──────────────────────────┐
                 ▼                                                           │
   (创建)     PENDING ──enqueue──▶ QUEUED ──获得槽位──▶ RUNNING ──成功──▶ SUCCEEDED
               │  │                  ▲                   │  │
 handler 缺失  │  │  退避结束重新入队  │                   │  ├─永久失败/重试用尽─▶ FAILED
  /队列满拒绝  │  └──────retry───────┴──── RETRYING ◀─────┘  └─────取消──────▶ CANCELLED
               ▼                         │  │
            FAILED                   退避中取消
            CANCELLED                     ▼
                                      CANCELLED
```

合法流转（由 `task_api/models.py::ALLOWED_TRANSITIONS` 强制校验，
任何非法流转都会抛 `INVALID_STATE_TRANSITION`）：

```
PENDING  -> QUEUED | CANCELLED | FAILED
QUEUED   -> RUNNING | CANCELLED
RUNNING  -> SUCCEEDED | FAILED | RETRYING | CANCELLED
RETRYING -> QUEUED | CANCELLED | FAILED
SUCCEEDED / FAILED / CANCELLED 为终态，不再流转
```

- **PENDING**：已建任务记录、尚未入队（处理器缺失或队列满时可在此终态留痕）。
- **QUEUED**：已入队（等待槽位或等待 worker 拉起）。
- **RUNNING**：已占用一个并发槽位，正在执行处理器。
- **RETRYING**：瞬时失败，指数退避中；槽位仍由该任务持有，退避结束回到 QUEUED。
- **SUCCEEDED / FAILED / CANCELLED**：终态。

每次流转都生成一条 `TransitionEvent`（含 `seq / from / to / attempt /
reason / error_code / request_id / trace_id / at`），可通过任务查询接口看到完整链路。

## 3. 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/tasks` | 提交任务（202 Accepted），body 见下 |
| GET  | `/tasks/{task_id}` | 按任务 ID 查询（含状态历史） |
| GET  | `/idempotency/{idempotency_key}` | 按幂等键查询既有任务 |
| POST | `/tasks/{task_id}/cancel` | 取消排队/退避/运行中的任务 |
| GET  | `/health` | 健康检查 |

提交 body：

```json
{
  "task_type": "echo",
  "payload": {"value": 1},
  "idempotency_key": "order-123",
  "max_attempts": 3,
  "timeout_seconds": 5.0
}
```

- `max_attempts`、`timeout_seconds` 可省略，回退到全局配置。
- 支持请求头 `X-Request-ID`、`X-Trace-ID`；未提供则自动生成，并通过同名响应头回传。

### 错误码 → HTTP 状态

| 错误码 | HTTP | 含义 |
|--------|------|------|
| `IDEMPOTENCY_KEY_CONFLICT` | 409 | 同键异内容 |
| `QUEUE_FULL` | 503（含 `Retry-After: 1`） | 并发满且队列满，背压拒绝 |
| `TASK_NOT_FOUND` | 404 | 任务/幂等键不存在 |
| `TASK_NOT_CANCELLABLE` / `TASK_ALREADY_TERMINAL` | 409 | 当前状态不可取消 |
| `HANDLER_NOT_FOUND` | 422 | 未注册的 task_type |
| `PERMANENT_FAILURE` | 体现在任务状态 | 永久失败，不重试 |
| `TRANSIENT_FAILURE` | 体现在任务状态 | 瞬时失败，触发重试 |
| `RETRIES_EXHAUSTED` | 体现在任务状态 | 瞬时失败在最大尝试次数后仍未成功 |
| `TIMEOUT` | 体现在任务状态 | 执行超时（按配置归类瞬时/永久） |
| `CANCELLED` | 体现在任务状态 | 被取消 |

错误响应体形如：`{"error": {"code": "QUEUE_FULL", "message": "..."}}`。

## 4. 幂等规则

1. 服务端对请求**业务内容**（`task_type` + `payload`，规范化 JSON 后取 SHA-256）
   计算 `request_hash`，与 `idempotency_key` 一起保存。
2. 查重与创建在存储层**同一把锁内原子完成**（`TaskStore.find_or_create`），
   并发重复提交只会建出一条任务。
3. 同键 + 同 hash：直接返回既有任务当前视图（可能仍在运行，也可能已终态），
   处理器不会被再次执行。
4. 同键 + 不同 hash：抛 `IDEMPOTENCY_KEY_CONFLICT`，错误信息中带原任务 ID，
   与"正常幂等命中"明确区分。
5. 指纹只覆盖业务内容，不包含 `max_attempts` / `timeout_seconds` 等执行参数。

## 5. 重试与失败分类规则

处理器通过异常类型显式分类：

- `raise PermanentTaskError(...)`：永久失败 → 立即 `FAILED`，不再重试；
- `raise TransientTaskError(...)`：瞬时失败 → 进入 `RETRYING` 退避后重试；
- `asyncio.TimeoutError` / 执行超时：按 `timeout_is_transient` 归类；
- 其它未分类异常：按 `unknown_error_is_transient` 归类。

重试策略：

- 最大尝试次数 `max_attempts`（1 = 不重试）；用尽后以 `RETRIES_EXHAUSTED` 失败，
  `failure_class=TRANSIENT`（表明是"瞬时问题但已放弃"）。
- 指数退避：`min(retry_backoff_max_seconds, retry_backoff_base_seconds * 2**(attempt-1))`。
- 可选整体截止时间 `total_timeout_seconds`，超过后以 `TIMEOUT` 失败。

**副作用不重复**：处理器在不可重试的外部副作用真正发生时调用
`ctx.side_effect()` 上报。框架保证：只要 `side_effect_occurred=True`，
该次尝试之后的任何失败都被**强制升级为永久失败**，不再重试，避免重复扣款/重复发消息。

```python
async def charge(payload, ctx):
    await payment_client.charge(payload["order_id"])  # 外部副作用
    ctx.side_effect()                                  # 如实上报
    return {"charged": True}
```

## 6. 并发与背压模型

执行器内部用"并发槽位 + 有界等待队列"（全部计数只在同一把条件锁内变更）：

- `slots_taken`：已预留槽位（待启动 + 运行中），范围 `0..max_concurrency`；
- `waiting`：槽位满时的有界 FIFO 队列，容量 `queue_capacity`。

提交时在临界区内**同步**裁决（与时序无关、结果确定）：

1. `slots_taken < max_concurrency`：立即预留槽位；
2. 否则 `waiting < queue_capacity`：进入队尾；
3. 否则抛 `QUEUE_FULL`。

任务终态释放槽位时，按 FIFO 把队首提升为可运行，因此：
同时运行数绝不超过上限、等待顺序确定、既不静默丢弃也不超卖。

## 7. 配置项

可通过环境变量覆盖（前缀 `TASK_API_`），定义见 `task_api/config.py`：

| 配置 | 环境变量 | 默认 | 说明 |
|------|----------|------|------|
| `max_concurrency` | `TASK_API_MAX_CONCURRENCY` | 2 | 最大并发 RUNNING 数 |
| `queue_capacity` | `TASK_API_QUEUE_CAPACITY` | 4 | 有界等待队列容量 |
| `default_max_attempts` | `TASK_API_DEFAULT_MAX_ATTEMPTS` | 3 | 默认最大尝试次数 |
| `retry_backoff_base_seconds` | `TASK_API_RETRY_BACKOFF_BASE_SECONDS` | 0.05 | 退避基准 |
| `retry_backoff_max_seconds` | `TASK_API_RETRY_BACKOFF_MAX_SECONDS` | 2.0 | 退避上限 |
| `default_timeout_seconds` | `TASK_API_DEFAULT_TIMEOUT_SECONDS` | 5.0 | 单次执行超时（空=不限） |
| `total_timeout_seconds` | `TASK_API_TOTAL_TIMEOUT_SECONDS` | 空 | 整体截止（空=不限） |
| `timeout_is_transient` | `TASK_API_TIMEOUT_IS_TRANSIENT` | true | 超时归类为瞬时/永久 |
| `unknown_error_is_transient` | `TASK_API_UNKNOWN_ERROR_IS_TRANSIENT` | true | 未分类异常归类 |

## 8. 日志与可观测性

日志统一打印**请求标识、链路标识、任务标识、当前并发度与状态流转**，形如：

```
... INFO task_api.executor [req_id=req_.. trace_id=trace_.. task_id=task_..] \
    concurrency(running=1/2 queued=2/4) transition *->RUNNING attempt=1 reason=attempt 1
```

任务查询接口的 `history` 字段给出完整、有序、带原因与错误码的状态流转记录。

## 9. 本地验证

环境：Python 3.14 + uv（见 `pyproject.toml`）。

```bash
# 安装依赖
uv sync

# 运行全部单测（会在输出中打印请求标识、并发度、状态流转）
uv run pytest -q

# 只看某一类场景
uv run pytest tests/test_idempotency.py -q   # 幂等重复提交 / 同键冲突
uv run pytest tests/test_concurrency.py -q  # 并发上限 / FIFO / 背压 / 排队取消
uv run pytest tests/test_retry.py -q        # 瞬时/永久失败 / 重试 / 副作用 / 超时 / 取消
uv run pytest tests/test_api_http.py -q     # HTTP 状态码与错误码一致性

# 启动服务
uv run uvicorn main:app --reload
# 或：uv run fastapi dev main.py
```

手动验证：

```bash
# 提交
curl -s -X POST localhost:8000/tasks \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: req-demo-1' -H 'X-Trace-ID: trace-demo-1' \
  -d '{"task_type":"echo","payload":{"v":1},"idempotency_key":"demo-1"}'

# 重复提交（同键同内容）-> 返回同一 task_id
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"task_type":"echo","payload":{"v":1},"idempotency_key":"demo-1"}'

# 同键异内容 -> 409 IDEMPOTENCY_KEY_CONFLICT
curl -i -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"task_type":"echo","payload":{"v":2},"idempotency_key":"demo-1"}'

# 查询完整状态链路
curl -s localhost:8000/idempotency/demo-1 | python -m json.tool
```

内置演示处理器：`echo`（成功）、`flaky.transient`（总瞬时失败）、
`flaky.permanent`（总永久失败）、`slow`（可用 `payload.delay_seconds` 制造超时）。

## 10. 代码结构

```
task_api/
  config.py          # 配置项（环境变量）
  errors.py          # 错误码与异常分类
  models.py          # 状态机、流转表、请求/视图/事件模型
  tracing.py         # request_id / trace_id
  logging_setup.py   # 统一日志（请求标识 + 并发度 + 状态流转）
  idempotency.py     # 请求指纹
  handlers.py        # 处理器注册与 HandlerContext（side_effect/is_cancelled）
  storage.py         # 内存存储 + 状态机守卫（单锁原子查重/创建/流转）
  executor.py        # 槽位限流 + 有界 FIFO + 退避重试 + 取消
  service.py         # 提交/查询/取消的幂等编排（唯一权威入口）
  api.py             # FastAPI 路由、错误码映射、标识中间件
  app.py             # 装配与演示处理器
tests/               # 幂等 / 并发 / 重试 / HTTP 四组测试
```
