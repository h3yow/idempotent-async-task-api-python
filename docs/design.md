# 幂等异步任务 API 设计说明

一套带**幂等控制**、**并发限流（背压）**与**失败分类重试**的异步任务提交/执行服务。
无论调用方从哪个入口（HTTP 重放、并发洪峰、失败重试）进入，得到的结果都一致且可解释：
重复提交不产生重复任务/副作用，过载不静默丢弃也不超卖，瞬时失败可重试且重试链路可追踪。

## 1. 任务状态机

```
                 ┌──────────────────────────────────────────┐
                 ▼                                           │ 瞬时失败/超时
  受理      ┌─────────┐  获得槽位   ┌─────────┐  成功      ┌───────────┐
 ───────▶ │ PENDING │ ──────────▶ │ RUNNING │ ─────────▶ │ SUCCEEDED │
          │ 排队中   │             │ 执行中   │            └───────────┘
          └─────────┘             └─────────┘  永久失败/
               │  │                   │  │      重试耗尽  ┌────────┐
               │  │ 排队中取消        │  │  ────────────▶ │ FAILED │
               │  └──────────────────┼──┼───▶┐           └────────┘
               │                     │  │ 取消│           ┌───────────┐
               └─────────────────────┴──┴────┴─────────▶ │ CANCELLED │
                                                          └───────────┘
```

合法流转（代码中由 `ALLOWED_TRANSITIONS` 强制，非法流转直接报错）：

| 当前状态 | 允许流转到 | 触发条件 |
|---|---|---|
| `PENDING` | `RUNNING` | 调度器/提交路径分配到并发槽位 |
| `PENDING` | `CANCELLED` | 排队期间被取消 |
| `PENDING` | `FAILED` | 受理后、执行前判定不可恢复（如未知任务类型） |
| `RUNNING` | `SUCCEEDED` | 处理器正常返回 |
| `RUNNING` | `FAILED` | 永久失败，或瞬时失败重试次数耗尽 |
| `RUNNING` | `CANCELLED` | 执行中收到取消信号，在尝试边界落地 |
| `RUNNING` | `PENDING` | 瞬时失败/超时，**释放槽位并重新进入 FIFO 队尾**等待重试 |
| 三个终态 | — | 终态不可逆；重复提交只返回既有记录 |

每次流转都向 `TaskRecord.history` 追加一条
`(时间戳, request_id, 原状态, 新状态, 说明)`，因此超时、取消与重试之间的全部状态变化都可关联追踪。

## 2. 幂等规则

- 提交请求必须携带 `idempotency_key`。服务端对 **请求内容**（`task_type` + 规范化 JSON 后的
  `payload`）计算 SHA-256 指纹，与幂等键绑定保存（`request_id` 不参与指纹）。
- **同键同内容**（任意时刻、任意 `request_id`、任意入口）：返回同一任务记录，
  `created=false, reason=duplicate_replay`。任务只存在一条、只执行一条执行链，
  终态后重放直接返回既有结果，**绝不重新执行，绝不产生第二次副作用**。
- **同键异内容**：返回 `409 idempotency_content_conflict`，错误信息中同时给出
  既有指纹与本次指纹前缀，冲突原因可区分。
- 容量不足的拒绝发生在幂等登记**之前**：被 `503` 拒绝的请求不占用幂等键，
  客户端可在容量恢复后用同一个键安全重放。
- 取消接口同样幂等：对已终态任务重复取消返回原状态，不报错。

## 3. 并发与背压语义

容量 = 并发槽位 + 等待队列，均在 `Settings` 中配置：

- 有空闲槽位：容量判定与 `mark_running` 占用在**同一把锁的临界区**内完成，
  并立即启动执行链（`created_running`）。并发计数由集合维护，超卖会触发防御性断言。
- 槽位满、队列未满：进入 FIFO 队列尾部（`created_queued`）。单一调度循环在槽位释放时
  按 FIFO 取队首补位；重试返回的任务也重新入队尾，不会插队抢占新任务的额度。
- 槽位与队列都满：返回 `503 capacity_queue_full`（带 `Retry-After: 1`），
  响应体包含 `current_concurrency` 与 `queue_depth`，**不静默丢弃、不超额受理**。
- 任务到达任意终态后释放槽位并唤醒调度器；等待中被取消的任务在出队时被跳过清理。

## 4. 失败分类与重试

处理器通过抛出异常声明失败类别（见 `app/errors.py`）：

| 异常 | `error_code` | 类别 | 是否重试 |
|---|---|---|---|
| `TransientError` | `transient_failure` | transient | 是 |
| `TaskTimeoutError`（执行超过 `task_timeout_seconds`） | `timeout` | transient | 是 |
| 未声明的普通 `Exception` | `transient_failure` | transient | 是（防御性归类） |
| `PermanentError` | `permanent_failure` | permanent | 否，直接终结 |
| `UnknownTaskTypeError` | `unknown_task_type` | permanent | 否 |
| 取消 | `cancelled` | — | 终结为 `CANCELLED` |

- 最大尝试次数 = `1 + max_retries`。`attempts` 只在真实开始一次尝试时递增。
- 重试流程：`RUNNING -> PENDING`（记录失败码与说明）→ 释放槽位、重新入 FIFO 队列 →
  调度器再次分配槽位 → `PENDING -> RUNNING`。因此重试期间不占用并发额度，
  排队顺序对所有任务公平。
- **重试不产生重复副作用**是处理器契约：处理器须借助任务/业务标识使自身副作用幂等；
  永久失败必须在产生副作用**之前**判定。框架保证同一次尝试只被调用一次、
  成功后永不再执行。
- 记录中的 `failure_kind`（`transient`/`permanent`）、`error_code`、`error_message`
  与 history 备注（如 `transient(timeout); retry later (1/2)`、
  `permanent(...); no retry`、`retries exhausted`）使最终成败完全可解释。

## 5. 链路追踪与可观测日志

- 每个请求可带 `X-Request-Id` 头（缺省自动生成），贯穿受理日志、状态流转 history 与响应体。
- 执行器每次流转打印一行：
  `[req=<request_id> task=<id> type=<类型> attempt=<次数> running=<并发度> queue=<队列深度> -> <新状态>] <说明>`。
- HTTP 提交/冲突/过载日志同样包含请求标识、并发度与队列深度。
- 单测通过 `--log-cli-level=INFO` 可直接看到上述日志。

## 6. 配置项（`app/config.py: Settings`）

| 配置 | 默认值 | 含义 |
|---|---|---|
| `max_concurrency` | `2` | 同时处于 RUNNING 的任务上限（并发额度，不可超卖） |
| `max_queue_size` | `4` | PENDING 等待队列容量；满后新请求被 503 拒绝 |
| `task_timeout_seconds` | `5.0` | 单次尝试执行超时；超时归类为瞬时失败 |
| `max_retries` | `2` | 瞬时失败最大重试次数（总尝试 = 1 + max_retries） |
| `retry_backoff_seconds` | `0.01` | 重试退避基准（保留配置项） |

## 7. HTTP 接口

| 方法与路径 | 说明 | 关键结果 |
|---|---|---|
| `POST /tasks` | 提交任务（body：`idempotency_key`/`task_type`/`payload`；可带 `X-Request-Id`） | `202` + `created/reason/current_concurrency/queue_depth/task` |
| `GET /tasks/{id}` | 查询任务状态、结果、错误分类与完整 history | `200` / `404 task_not_found` |
| `POST /tasks/{id}/cancel` | 取消排队/执行中的任务（幂等） | `200` |
| `GET /health` | 健康检查 | `200` |

错误响应统一结构：`error_code`、`message`、`request_id`、`current_concurrency`、`queue_depth`。

## 8. 本地验证

```bash
uv sync --group dev

# 全量单测（17 个：幂等 3 / 并发 4 / 重试取消 5 / 超时 1 / HTTP 4）
uv run pytest -q

# 查看请求标识、并发度与状态流转日志
uv run pytest --log-cli-level=INFO

# 启动服务手工验证
uv run uvicorn main:app --port 8000

# 首次提交
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -H 'x-request-id: demo-1' \
  -d '{"idempotency_key":"demo","task_type":"flaky","payload":{"fail_times":2}}'
# 同键同内容重放 -> created=false, reason=duplicate_replay，同一 task_id
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key":"demo","task_type":"flaky","payload":{"fail_times":2}}'
# 同键不同内容 -> 409 idempotency_content_conflict
curl -s -i -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key":"demo","task_type":"flaky","payload":{"fail_times":3}}'
# 查询（观察 history 中 RUNNING->PENDING->RUNNING 的重试链路）
curl -s localhost:8000/tasks/<task_id>
```

内置演示处理器：`echo`（回显，无副作用）、`flaky`
（`payload.fail_times` 控制前 N 次瞬时失败；`payload.permanent=true` 直接永久失败）。

## 9. 代码结构

| 文件 | 职责 |
|---|---|
| `app/models.py` | 状态枚举、合法流转表、请求指纹、任务记录 |
| `app/errors.py` | 稳定错误码：冲突/过载/瞬时/永久/超时/取消 |
| `app/store.py` | 幂等键映射、FIFO 队列、并发计数（单异步锁保护） |
| `app/handlers.py` | 处理器注册表与执行上下文 |
| `app/executor.py` | 调度循环、槽位占用、超时、退避重试、取消与终态通知 |
| `app/service.py` | 幂等判定与容量受理编排 |
| `app/api.py` | FastAPI 路由与可区分的错误响应 |
| `app/wiring.py` / `main.py` | 装配与启动 |
