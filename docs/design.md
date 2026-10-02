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
| `PENDING` | `FAILED` | 受理后、执行前判定不可恢复（如未知任务类型、整体预算已用尽） |
| `RUNNING` | `SUCCEEDED` | 处理器正常返回 |
| `RUNNING` | `FAILED` | 永久失败，或瞬时失败重试次数耗尽，或整体时间预算用尽 |
| `RUNNING` | `ABORTED` | **不可重复的副作用已生效**后又失败/超时：不再重试，提前终结 |
| `RUNNING` | `CANCELLED` | 执行中收到取消信号，在尝试边界落地 |
| `RUNNING` | `PENDING` | 瞬时失败/超时，**释放槽位并重新进入 FIFO 队尾**等待重试 |
| 四个终态 | — | 终态不可逆；重复提交只返回既有记录 |

每次流转都向 `TaskRecord.history` 追加一条
`(时间戳, request_id, 原状态, 新状态, 说明)`，因此超时、取消与重试之间的全部状态变化都可关联追踪。

## 2. 幂等规则

- 提交请求必须携带 `idempotency_key`。服务端对 **请求内容**（`task_type` + 规范化 JSON 后的
  `payload` + 覆盖参数 `max_retries`/`time_budget_seconds`）计算 SHA-256 指纹，
  与幂等键绑定保存（`request_id` 不参与指纹）。
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
- 记录中的 `failure_kind`（`transient`/`permanent`/`side_effect_committed`/
  `budget_exhausted`）、`error_code`、`error_message`
  与 history 备注（如 `transient(timeout); retry later (1/2)`、
  `permanent(...); no retry`、`retries exhausted`）使最终成败完全可解释。

## 4.1 副作用保护（不可重复动作的重试保护）

有一类任务的动作**一旦发生就绝不能做第二次**（扣款、发通知、落外部单据）。
这类处理器在动作实际生效后应立即调用 `ctx.commit_side_effect()`：

- 标记之后，本次尝试若再失败或超时，任务**不再重试**，直接以终态 `ABORTED` 结束：
  - `error_code = side_effect_committed`，`failure_kind = side_effect_committed`，
    与「什么都没发生就失败」（`FAILED` + transient/permanent）和「重试耗尽」明确区分；
  - 语义：动作已生效但整体没跑完，需要人工或补偿介入，调用方一眼可辨。
- 标记**之前**的失败不受影响：仍按第 4 节规则正常重试，不会误伤。
- `ABORTED` 与其他终态一样**正常归还并发槽位**并唤醒调度器，排队任务继续执行。
- `ABORTED` 后同键同内容重放仍复用同一记录（`duplicate_replay`），
  绝不会把动作再做一遍。

## 4.2 提交级覆盖参数与整体时间预算

提交时可**可选**指定两个覆盖参数（不指定则完全沿用 `Settings` 默认值，老调用方不受影响）：

| 参数 | 范围 | 默认 | 含义 |
|---|---|---|---|
| `max_retries` | 整数 `0..10` | `Settings.max_retries`（2） | 瞬时失败最大重试次数；`0` 表示尽快失败 |
| `time_budget_seconds` | 浮点 `0.1..3600` | 不限制 | 整体时间预算（秒），自首次受理起算 |

- 非法取值（超范围、类型错误）返回 `400 invalid_parameter`，错误信息说明具体原因。
- 整体预算用尽是可区分的失败原因：`error_code = budget_exhausted`、
  `failure_kind = budget_exhausted`，与单次执行超时（`timeout`）不同。
  单次尝试的超时阈值会被剩余预算收紧；剩余预算先耗尽时按预算用尽归类。
- 覆盖参数**参与幂等指纹**：同键但覆盖取值不同视为不同内容，返回 409；
  同键同参重放仍复用同一任务，不重复执行、不重复产生副作用。
- 覆盖后的取值在 `TaskView`（`max_retries`/`time_budget_seconds`）与
  history 备注中可见。

## 4.3 幂等键反查

`GET /tasks/by-key/{idempotency_key}`：客户端丢失提交响应后，凭同一幂等键查回任务。

- 返回与按 ID 查询相同的 `TaskView`，其中：
  - `state`/`attempts`/`error_code`/`failure_kind`：当前状态、已尝试次数与失败原因；
  - `side_effect_committed`：是否已产生过不可重复的副作用；
  - `will_retry`：是否仍可能发起新的尝试（终态一律为 `false`）。
- 查无此键返回 `404 task_not_found`（结构化错误体，不是 500，也不是空响应）。
- 反查是**纯查询**：不触发任何执行或副作用，也不影响同键异内容的 409 拒绝规则。

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
| `POST /tasks` | 提交任务（body：`idempotency_key`/`task_type`/`payload`，可选 `max_retries`/`time_budget_seconds`；可带 `X-Request-Id`） | `202` + `created/reason/current_concurrency/queue_depth/task`；参数非法 `400 invalid_parameter` |
| `GET /tasks/{id}` | 查询任务状态、结果、错误分类与完整 history | `200` / `404 task_not_found` |
| `GET /tasks/by-key/{key}` | 按幂等键反查任务（纯查询，含 `side_effect_committed`/`will_retry`） | `200` / `404 task_not_found` |
| `POST /tasks/{id}/cancel` | 取消排队/执行中的任务（幂等） | `200` |
| `GET /health` | 健康检查 | `200` |

错误响应统一结构：`error_code`、`message`、`request_id`、`current_concurrency`、`queue_depth`。

## 8. 本地验证

```bash
uv sync --group dev

# 全量单测（35 个：幂等 3 / 并发 4 / 重试取消 5 / 超时 1 / HTTP 4
#              / 副作用保护 5 / 幂等键反查 5 / 覆盖参数 8）
uv run pytest -q

# 查看请求标识、并发度、状态流转与副作用次数日志
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

# 副作用保护：动作生效后失败 -> ABORTED，side_effect_committed=true，不再重试
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key":"charge-1","task_type":"charge_once","payload":{"after_commit":"transient"}}'
# 幂等键反查（丢失响应后凭键查回状态）
curl -s localhost:8000/tasks/by-key/charge-1

# 覆盖参数：0 次重试 + 整体预算 2 秒；预算用尽 -> budget_exhausted
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key":"ov-1","task_type":"flaky","payload":{"fail_times":99},
       "max_retries":0,"time_budget_seconds":2}'
# 非法覆盖 -> 400 invalid_parameter
curl -s -i -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key":"ov-2","task_type":"flaky","max_retries":99}'
```

内置演示处理器：`echo`（回显，无副作用）、`flaky`
（`payload.fail_times` 控制前 N 次瞬时失败；`payload.permanent=true` 直接永久失败）、
`charge_once`（动作生效后 `commit_side_effect()`；`payload.after_commit` 为
`transient`/`timeout` 时演示 ABORTED 终结）。

## 9. 代码结构

| 文件 | 职责 |
|---|---|
| `app/models.py` | 状态枚举（含 ABORTED）、合法流转表、请求指纹、任务记录 |
| `app/errors.py` | 稳定错误码：冲突/过载/瞬时/永久/超时/取消/副作用已生效/预算用尽/参数非法 |
| `app/store.py` | 幂等键映射、FIFO 队列、并发计数（单异步锁保护） |
| `app/handlers.py` | 处理器注册表与执行上下文（含 `commit_side_effect()`） |
| `app/executor.py` | 调度循环、槽位占用、超时、整体预算、副作用保护、退避重试、取消与终态通知 |
| `app/service.py` | 幂等判定、覆盖参数校验与容量受理编排 |
| `app/api.py` | FastAPI 路由（含幂等键反查）与可区分的错误响应 |
| `app/wiring.py` / `main.py` | 装配与启动 |
