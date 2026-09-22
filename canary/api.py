"""FastAPI 路由：发布计划、受控推进、人工动作、健康注入与兼容性查询。

所有拒绝/失败都返回可区分的 ``error`` 代码；健康异常触发的自动回滚会在
错误响应体中同时返回回滚后的状态（``state_after``），便于本地断言与解释。
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from .errors import OrchestratorError
from .models import (
    AdvanceResult,
    ReleasePlan,
    ReleaseState,
    WeightSplit,
)

logger = logging.getLogger("canary.api")

router = APIRouter(prefix="/releases", tags=["canary"])


# ---------------------------------------------------------------- 请求模型

class CreateReleaseRequest(BaseModel):
    candidate_version: str = Field(min_length=1, examples=["2.0.0"])
    stable_version: str = Field(min_length=1, examples=["1.0.0"])
    batch_weights: list[int] = Field(examples=[[20, 60, 100]], min_length=1)
    min_observation_seconds: float = Field(default=0, ge=0)


class RollbackRequest(BaseModel):
    reason: str = "人工回滚"


class AbortRequest(BaseModel):
    reason: str = "人工中止"


class InjectHealthRequest(BaseModel):
    outcome: str = Field(pattern="^(healthy|unhealthy|timeout)$")
    reason: str = ""


# ---------------------------------------------------------------- 依赖

def get_orchestrator(request: Request):
    return request.app.state.orchestrator


def _result_payload(result: AdvanceResult) -> dict[str, Any]:
    return result.model_dump(mode="json")


def _error_response(exc: OrchestratorError) -> dict[str, Any]:
    body: dict[str, Any] = exc.to_dict()
    state_after = getattr(exc, "state_after", None)
    if isinstance(state_after, ReleaseState):
        body["state_after"] = state_after.model_dump(mode="json")
        body["weight_split"] = WeightSplit(
            candidate_version=state_after.candidate_version,
            stable_version=state_after.stable_version,
            candidate_weight=state_after.current_weight,
            stable_weight=100 - state_after.current_weight,
        ).model_dump(mode="json")
        body["rolled_back"] = True
    return body


# ---------------------------------------------------------------- 接口

@router.post("", status_code=201)
def create_release(payload: CreateReleaseRequest, request: Request) -> dict[str, Any]:
    orch = get_orchestrator(request)
    plan = ReleasePlan(
        release_id=_new_release_id(orch),
        candidate_version=payload.candidate_version,
        stable_version=payload.stable_version,
        batch_weights=payload.batch_weights,
        min_observation_seconds=payload.min_observation_seconds,
    )
    state = orch.create_release(plan)
    logger.info("[api] POST /releases 创建 release=%s", state.release_id)
    return {"release_id": state.release_id, "state": state.model_dump(mode="json")}


def _new_release_id(orch) -> str:
    import uuid

    # 避免极小概率碰撞：生成后检查存在性
    for _ in range(5):
        rid = f"rel-{uuid.uuid4().hex[:12]}"
        try:
            orch.get(rid)
        except OrchestratorError:
            return rid
    return f"rel-{uuid.uuid4().hex}"


@router.get("")
def list_releases(request: Request) -> dict[str, Any]:
    orch = get_orchestrator(request)
    states = orch.list_releases()
    return {"releases": [s.model_dump(mode="json") for s in states]}


@router.get("/compat")
def compatibility_report(request: Request) -> dict[str, Any]:
    """状态数据兼容性报告：schema 版本与被隔离/告警记录。"""
    orch = get_orchestrator(request)
    return orch.store.compat_report()


@router.get("/{release_id}")
def get_release(release_id: str, request: Request) -> dict[str, Any]:
    orch = get_orchestrator(request)
    state = orch.get(release_id)
    return {"state": state.model_dump(mode="json")}


@router.post("/{release_id}/advance")
def advance_release(release_id: str, request: Request) -> dict[str, Any]:
    orch = get_orchestrator(request)
    result = orch.advance(release_id)
    logger.info("[api] advance release=%s action=%s", release_id, result.action)
    return _result_payload(result)


@router.post("/{release_id}/pause")
def pause_release(release_id: str, request: Request) -> dict[str, Any]:
    return _result_payload(get_orchestrator(request).pause(release_id))


@router.post("/{release_id}/resume")
def resume_release(release_id: str, request: Request) -> dict[str, Any]:
    return _result_payload(get_orchestrator(request).resume(release_id))


@router.post("/{release_id}/rollback")
def rollback_release(release_id: str, payload: RollbackRequest, request: Request) -> dict[str, Any]:
    return _result_payload(
        get_orchestrator(request).rollback(release_id, reason=payload.reason)
    )


@router.post("/{release_id}/abort")
def abort_release(release_id: str, payload: AbortRequest, request: Request) -> dict[str, Any]:
    return _result_payload(
        get_orchestrator(request).abort(release_id, reason=payload.reason)
    )


@router.post("/{release_id}/health-injection")
def inject_health(release_id: str, payload: InjectHealthRequest, request: Request) -> dict[str, Any]:
    """为本地健康门禁预置下一次探测结果（healthy/unhealthy/timeout）。"""
    orch = get_orchestrator(request)
    # 确保 release 存在，不存在时给出与其他接口一致的可区分错误
    orch.get(release_id)
    gate = orch.gate
    if payload.outcome == "healthy":
        gate.inject_healthy(release_id, payload.reason or "注入：健康")
    elif payload.outcome == "unhealthy":
        gate.inject_unhealthy(release_id, payload.reason or "注入：不健康")
    else:
        gate.inject_timeout(release_id)
    logger.info("[api] inject release=%s outcome=%s", release_id, payload.outcome)
    return {"release_id": release_id, "injected": payload.outcome}


def get_data_path() -> str:
    return os.environ.get("CANARY_STATE_FILE", "data/releases.json")
