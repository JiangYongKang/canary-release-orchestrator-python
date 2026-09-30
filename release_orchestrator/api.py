"""HTTP 路由：将编排器能力暴露为本地 REST 接口。

异常 -> HTTP 映射（错误码可程序化区分）：
- release_not_found       404
- duplicate_release       409  重复提交（同名发布）
- illegal_transition      409  非法状态转换
- duplicate_advance       409  观察期未满/暂停态直接推进/重复中止（本次未生效）
- advance_conflict        409  并发冲突（同进程在途 / 其他实例正操作同一发布，本次未生效）
- stale_state             409  基于过期进度的变更被最新共享状态拒绝（本次未生效）
- manual_abort            409  对已人工中止发布执行操作
- storage_busy            503  共享状态提交锁在限定时间内不可用（本次未生效，可重试）
- probe_timeout           504  探测超时（已自动回滚，响应体含回滚后状态）
- consecutive_failure     502  连续健康失败达阈值（已自动回滚，响应体含回滚后状态）
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .errors import (
    AdvanceConflictError,
    ConsecutiveFailureError,
    DuplicateAdvanceError,
    DuplicateReleaseError,
    IllegalTransitionError,
    ManualAbortError,
    OrchestratorError,
    ProbeTimeoutError,
    ReleaseNotFoundError,
    StaleStateError,
    StorageBusyError,
)
from .models import GateAction, ReleasePlan
from .schemas import (
    AbortRequest,
    AdvanceResponse,
    CreateReleaseRequest,
    EventView,
    ReleaseView,
)

logger = logging.getLogger("release_orchestrator.api")

router = APIRouter()

_STATUS_MAP = {
    ReleaseNotFoundError: 404,
    DuplicateReleaseError: 409,
    IllegalTransitionError: 409,
    DuplicateAdvanceError: 409,
    AdvanceConflictError: 409,
    StaleStateError: 409,
    ManualAbortError: 409,
    StorageBusyError: 503,
    ProbeTimeoutError: 504,
    ConsecutiveFailureError: 502,
}


def _error_response(exc: OrchestratorError, request: Request | None = None) -> JSONResponse:
    status_code = _STATUS_MAP.get(type(exc), 400)
    body: dict = {"error_code": exc.error_code, "message": str(exc), "effective": False}
    # 超时/连续失败触发了自动回滚：把回滚后的一致状态一并返回，便于解释
    record = getattr(exc, "record", None)
    rolled_back = isinstance(exc, (ProbeTimeoutError, ConsecutiveFailureError))
    if record is not None and rolled_back:
        body["action"] = GateAction.ROLLED_BACK.value
        body["release"] = _record_to_view(record)
    else:
        # stale_state / 冲突类拒绝：附上共享状态中该发布的最新视图，
        # 让调用方无需再次查询即可区分"未生效"并看到别人推进到了哪里
        rid = getattr(exc, "release_id", None)
        if rid is None and request is not None:
            rid = request.path_params.get("release_id")
        if rid is not None:
            try:
                latest = request.app.state.orchestrator.get_release(rid)
                body["release"] = _record_to_view(latest)
            except Exception:  # noqa: BLE001 - 附加信息失败不影响错误响应
                pass
    logger.warning("api reject: code=%s http=%d message=%s",
                   exc.error_code, status_code, exc)
    return JSONResponse(status_code=status_code, content=body)


def _record_to_view(record) -> dict:
    return ReleaseView(
        id=record.id,
        name=record.plan.name,
        candidate=record.plan.candidate,
        stable=record.plan.stable,
        weights=list(record.plan.weights),
        min_observe_seconds=record.plan.min_observe_seconds,
        status=record.status.value,
        current_batch=record.current_batch,
        current_weight=record.current_weight,
        active_version=record.active_version,
        completed_batches=record.completed_batches,
        rollback_reason=record.rollback_reason,
        created_at=record.created_at,
        updated_at=record.updated_at,
        version=getattr(record, "version", 0),
        batches=[
            {
                "index": i + 1,
                "weight": b.weight,
                "completed": b.completed,
                "started_at": b.started_at,
                "completed_at": b.completed_at,
                "gate_attempts": b.gate_attempts,
                "consecutive_failures": b.consecutive_failures,
            }
            for i, b in enumerate(record.batches)
        ],
    ).model_dump()


@router.post("/releases", status_code=201, response_model=ReleaseView)
async def create_release(body: CreateReleaseRequest, request: Request):
    orch = request.app.state.orchestrator
    plan = ReleasePlan(
        name=body.name,
        candidate=body.candidate,
        stable=body.stable,
        weights=list(body.weights),
        min_observe_seconds=body.min_observe_seconds,
        max_consecutive_failures=body.max_consecutive_failures,
    )
    logger.info("api input POST /releases: name=%s candidate=%s stable=%s weights=%s observe=%s",
                body.name, body.candidate, body.stable, body.weights, body.min_observe_seconds)
    try:
        record = orch.create_release(plan)
    except (DuplicateReleaseError, ValueError) as exc:
        if isinstance(exc, ValueError) and not isinstance(exc, OrchestratorError):
            return JSONResponse(
                status_code=422,
                content={"error_code": "invalid_plan", "message": str(exc)},
            )
        return _error_response(exc, request)
    return _record_to_view(record)


@router.get("/releases")
async def list_releases(request: Request):
    orch = request.app.state.orchestrator
    return {"releases": [_record_to_view(r) for r in orch.list_releases()]}


@router.get("/releases/{release_id}", response_model=ReleaseView)
async def get_release(release_id: str, request: Request):
    try:
        record = request.app.state.orchestrator.get_release(release_id)
    except ReleaseNotFoundError as exc:
        return _error_response(exc, request)
    return _record_to_view(record)


@router.post("/releases/{release_id}/advance", response_model=AdvanceResponse)
async def advance_release(release_id: str, request: Request):
    orch = request.app.state.orchestrator
    logger.info("api input POST /releases/%s/advance", release_id)
    try:
        record, action = await orch.advance(release_id)
    except OrchestratorError as exc:
        return _error_response(exc, request)
    batch = record.batches[record.current_batch - 1] if record.current_batch > 0 else None
    detail = {
        "status": record.status.value,
        "current_batch": record.current_batch,
        "candidate_weight": record.current_weight,
        "active_version": record.active_version,
        "gate_attempts": batch.gate_attempts if batch else 0,
        "consecutive_failures": batch.consecutive_failures if batch else 0,
    }
    return {"release": _record_to_view(record), "action": action.value, "detail": detail}


@router.post("/releases/{release_id}/abort", response_model=ReleaseView)
async def abort_release(release_id: str, body: AbortRequest, request: Request):
    logger.info("api input POST /releases/%s/abort reason=%s", release_id, body.reason)
    try:
        record = await request.app.state.orchestrator.abort(release_id, body.reason)
    except OrchestratorError as exc:
        return _error_response(exc, request)
    return _record_to_view(record)


@router.post("/releases/{release_id}/resume", response_model=ReleaseView)
async def resume_release(release_id: str, request: Request):
    logger.info("api input POST /releases/%s/resume", release_id)
    try:
        record = await request.app.state.orchestrator.resume(release_id)
    except OrchestratorError as exc:
        return _error_response(exc, request)
    return _record_to_view(record)


@router.get("/releases/{release_id}/events")
async def get_events(release_id: str, request: Request):
    try:
        events = request.app.state.orchestrator.get_events(release_id)
    except ReleaseNotFoundError as exc:
        return _error_response(exc, request)
    return {"release_id": release_id, "events": events}


@router.get("/health")
async def health(request: Request):
    """服务健康自检：同时显式报告被隔离的不兼容历史数据。"""
    orch = request.app.state.orchestrator
    return {
        "status": "ok",
        "quarantined_count": len(orch.quarantined),
        "quarantined": [
            {"release_id": q.release_id, "reason": q.reason, "quarantined_at": q.quarantined_at}
            for q in orch.quarantined
        ],
    }
