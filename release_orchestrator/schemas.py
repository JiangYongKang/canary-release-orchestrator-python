"""HTTP 请求/响应模型（骨架：仅类型定义）。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CreateReleaseRequest(BaseModel):
    name: str = Field(min_length=1)
    candidate: str = Field(min_length=1)
    stable: str = Field(min_length=1)
    weights: list[int] = Field(min_length=1)
    min_observe_seconds: float = Field(gt=0)
    max_consecutive_failures: int = Field(default=3, ge=1)


class BatchInfo(BaseModel):
    index: int
    weight: int
    completed: bool
    started_at: float | None
    completed_at: float | None
    gate_attempts: int
    consecutive_failures: int


class ReleaseView(BaseModel):
    id: str
    name: str
    candidate: str
    stable: str
    weights: list[int]
    min_observe_seconds: float
    status: str
    current_batch: int
    current_weight: int
    active_version: str
    completed_batches: int
    rollback_reason: str | None
    created_at: float
    updated_at: float
    batches: list[BatchInfo]
    # 乐观版本号：每次生效变更（含陈旧拒绝事件）单调 +1。
    # 多实例调用方可在推进/恢复/中止时回传以检测陈旧进度。
    version: int = 1


class AdvanceRequest(BaseModel):
    """推进请求体（可选）：回传上次查询到的 version 做乐观并发校验。

    不传时保持原语义：仅按共享状态当前阶段判定。
    """

    expected_version: int | None = Field(default=None, ge=1)


class AdvanceResponse(BaseModel):
    release: ReleaseView
    action: str
    detail: dict[str, Any] = Field(default_factory=dict)


class AbortRequest(BaseModel):
    reason: str = ""
    expected_version: int | None = Field(default=None, ge=1)


class ResumeRequest(BaseModel):
    expected_version: int | None = Field(default=None, ge=1)


class EventView(BaseModel):
    seq: int
    timestamp: float
    type: str
    detail: dict[str, Any]


class ErrorResponse(BaseModel):
    error_code: str
    message: str
