"""发布编排的数据模型与状态定义（纯类型，不含业务逻辑）。"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator

SCHEMA_VERSION = 1
"""状态数据格式版本，持久化时一并写入，用于兼容性判定。"""


def utcnow() -> datetime:
    """返回带时区的当前 UTC 时间。"""
    return datetime.now(timezone.utc)


class ReleaseStatus(str, Enum):
    """发布状态机。

    合法流转见 ``ReleaseStatus.can_transition_to``。
    """

    PENDING = "pending"        # 已创建，尚未开始任何批次
    PROGRESSING = "progressing"  # 正在按批次推进
    PAUSED = "paused"          # 被健康门禁或人工暂停
    COMPLETED = "completed"    # 全部批次完成，候选版本成为稳定版本
    ROLLED_BACK = "rolled_back"  # 已回滚，流量全部回到稳定版本
    ABORTED = "aborted"        # 人工中止（终态，不自动恢复）

    def is_terminal(self) -> bool:
        return self in (ReleaseStatus.COMPLETED, ReleaseStatus.ABORTED, ReleaseStatus.ROLLED_BACK)

    def can_transition_to(self, target: "ReleaseStatus") -> bool:
        allowed: dict[ReleaseStatus, frozenset[ReleaseStatus]] = {
            ReleaseStatus.PENDING: frozenset(
                {ReleaseStatus.PROGRESSING, ReleaseStatus.ABORTED}
            ),
            ReleaseStatus.PROGRESSING: frozenset(
                {
                    ReleaseStatus.PAUSED,
                    ReleaseStatus.COMPLETED,
                    ReleaseStatus.ROLLED_BACK,
                    ReleaseStatus.ABORTED,
                }
            ),
            ReleaseStatus.PAUSED: frozenset(
                {
                    ReleaseStatus.PROGRESSING,
                    ReleaseStatus.ROLLED_BACK,
                    ReleaseStatus.ABORTED,
                }
            ),
            ReleaseStatus.COMPLETED: frozenset(),
            ReleaseStatus.ROLLED_BACK: frozenset(),
            ReleaseStatus.ABORTED: frozenset(),
        }
        return target in allowed[self]


class GateDecision(str, Enum):
    """健康门禁判定结果。"""

    CONTINUE = "continue"  # 观察通过，可进入下一批次
    PAUSE = "pause"        # 指标可疑，暂停发布等待人工处置
    ROLLBACK = "rollback"  # 健康不达标，立即回滚


class FailureKind(str, Enum):
    """健康检查失败的可识别异常类型。"""

    TIMEOUT = "probe_timeout"        # 探测超时
    CONSECUTIVE_FAILURE = "consecutive_failures"  # 连续失败超过阈值
    MANUAL_ABORT = "manual_abort"    # 人工中止


class BatchRecord(BaseModel):
    """单个流量批次的执行记录。"""

    index: int
    weight: int = Field(ge=0, le=100)
    started_at: datetime | None = None
    observed_at: datetime | None = None
    gate_decision: GateDecision | None = None
    completed: bool = False


class HealthSample(BaseModel):
    """一次注入的健康/指标样本（测试或本地探针写入）。"""

    healthy: bool
    reason: str = ""
    sampled_at: datetime = Field(default_factory=utcnow)


class GateEvaluation(BaseModel):
    """门禁判定的结构化结果，可解释每一步依据。"""

    decision: GateDecision
    reason: str
    failure_kind: FailureKind | None = None
    consecutive_failures: int = 0
    sample: HealthSample | None = None


class ReleasePlan(BaseModel):
    """发布计划定义。"""

    release_id: str = Field(min_length=1)
    candidate_version: str = Field(min_length=1)
    stable_version: str = Field(min_length=1)
    # 每一步候选版本承担的流量百分比，必须严格递增且末项为 100
    batch_weights: list[int] = Field(min_length=1)
    min_observation_seconds: float = Field(ge=0, default=0)

    @field_validator("batch_weights")
    @classmethod
    def _validate_weights(cls, v: list[int]) -> list[int]:
        if any(w < 0 or w > 100 for w in v):
            raise ValueError("batch_weights 必须位于 [0, 100]")
        if any(v[i] >= v[i + 1] for i in range(len(v) - 1)):
            raise ValueError("batch_weights 必须严格递增")
        if v[-1] != 100:
            raise ValueError("batch_weights 最后一项必须为 100（全量）")
        return v

    @model_validator(mode="after")
    def _validate_versions_distinct(self) -> "ReleasePlan":
        if self.candidate_version == self.stable_version:
            raise ValueError("candidate_version 与 stable_version 不能相同")
        return self


class ReleaseState(BaseModel):
    """持久化的发布状态（重启恢复的唯一事实来源）。"""

    schema_version: int = SCHEMA_VERSION
    release_id: str
    candidate_version: str
    stable_version: str
    batch_weights: list[int]
    min_observation_seconds: float = 0

    status: ReleaseStatus = ReleaseStatus.PENDING
    # 已完成门禁观察的批次数量；下一个推进的批次索引 = completed_batches
    completed_batches: int = 0
    current_weight: int = 0  # 当前候选版本流量权重；稳定版本权重 = 100 - current_weight
    active_version: str | None = None  # 完成后记录最终对外版本

    batches: list[BatchRecord] = Field(default_factory=list)
    last_gate: GateEvaluation | None = None
    last_failure: FailureKind | None = None
    rollback_reason: str | None = None

    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    def effective_stable_version(self) -> str:
        """回滚后流量指向的稳定版本。

        若上一次发布已完成（候选转正），当前 stable_version 即为该版本。
        """
        return self.stable_version


class WeightSplit(BaseModel):
    """当前流量权重分配（两数之和恒为 100，保证可解释、一致）。"""

    candidate_version: str
    stable_version: str
    candidate_weight: int = Field(ge=0, le=100)
    stable_weight: int = Field(ge=0, le=100)


class AdvanceResult(BaseModel):
    """一次推进调用的结构化结果。

    - action 描述本调用实际产生的动作（未推进时调用方会收到可区分异常）；
    - gate 为本批次门禁结论；
    - rolled_back=True 表示因健康异常已在本次调用中完成自动回滚；
      此时调用方还会收到携带 ``state_after`` 的具体健康异常。
    """

    release_id: str
    action: str
    message: str
    state: ReleaseState
    gate: GateEvaluation | None = None
    weight_split: WeightSplit
    rolled_back: bool = False
