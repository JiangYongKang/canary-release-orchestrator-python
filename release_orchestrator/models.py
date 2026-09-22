"""领域模型与状态定义（骨架：仅类型定义）。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ReleaseStatus(str, Enum):
    """发布生命周期状态。"""

    PENDING = "pending"          # 已创建，尚未推进
    IN_PROGRESS = "in_progress"  # 金丝雀推进中（已完成部分批次）
    PAUSED = "paused"            # 健康门禁暂停（人工恢复后可继续）
    COMPLETED = "completed"      # 全部批次完成，候选版本已全量
    ROLLED_BACK = "rolled_back"  # 已回滚，100% 流量回到稳定版本
    ABORTED = "aborted"          # 人工中止（终态，不自动回滚）


TERMINAL_STATUSES = frozenset(
    {ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK, ReleaseStatus.ABORTED}
)


class HealthVerdict(str, Enum):
    """健康/指标探测的判定结果。"""

    HEALTHY = "healthy"      # 通过：允许推进到下一批次
    DEGRADED = "degraded"    # 降级：暂停推进，保持当前权重
    UNHEALTHY = "unhealthy"  # 不健康：立即回滚


class GateAction(str, Enum):
    """单次推进产生的动作。"""

    STARTED = "started"        # 发布启动，切换到第一批权重
    ADVANCED = "advanced"      # 观察期满足且门禁通过，推进到下一批
    COMPLETED = "completed"    # 末批门禁通过，候选版本全量
    PAUSED = "paused"          # 门禁判定降级，进入暂停
    STILL_WAITING = "waiting"  # 观察期未满，本次不产生切换
    ROLLED_BACK = "rolled_back"  # 门禁失败 / 连续失败 / 探测超时，触发回滚


@dataclass
class HealthProbeResult:
    """一次健康探测的结果。"""

    verdict: HealthVerdict
    reason: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchState:
    """单个批次的执行状态。"""

    weight: int                  # 本批次候选版本权重（0-100）
    completed: bool = False      # 是否已完成（切权重 + 观察期 + 门禁均通过）
    started_at: float | None = None
    completed_at: float | None = None
    gate_attempts: int = 0
    consecutive_failures: int = 0  # 连续不健康计数（跨重启持久化，健康判定后清零）


@dataclass
class ReleasePlan:
    """发布计划定义。"""

    name: str
    candidate: str
    stable: str
    weights: list[int]            # 分批流量权重，最后一批必须为 100
    min_observe_seconds: float    # 最小观察期
    max_consecutive_failures: int = 3  # 连续失败阈值


@dataclass
class ReleaseRecord:
    """一条发布记录（持久化单元）。"""

    id: str
    plan: ReleasePlan
    status: ReleaseStatus
    current_batch: int
    batches: list[BatchState]
    current_weight: int
    active_version: str
    created_at: float
    updated_at: float
    completed_batches: int = 0
    rollback_reason: str | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
