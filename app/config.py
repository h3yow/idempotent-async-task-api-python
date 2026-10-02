"""服务配置项：并发上限、队列容量、超时、重试等。"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # 同时执行任务数上限（并发额度，不得超卖）
    max_concurrency: int = 2
    # 等待队列容量；超出以明确原因拒绝
    max_queue_size: int = 4
    # 单任务执行超时（秒）
    task_timeout_seconds: float = 5.0
    # 瞬时失败最大重试次数（不含首次执行）
    max_retries: int = 2
    # 每次重试前的基础退避（秒）
    retry_backoff_seconds: float = 0.01
    # 提交时可选覆盖参数的合法上限（下限固定：max_retries >= 0，预算 > 0）
    max_allowed_retries: int = 10
    max_time_budget_seconds: float = 3600.0
