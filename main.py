"""服务入口。

保持原有 ``GET / -> {"Hello": "World"}`` 行为不变，并挂载渐进式发布编排能力。
状态文件路径可用环境变量 ``CANARY_STATE_FILE`` 覆盖（默认 data/releases.json）。
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from canary.api import get_data_path, router as canary_router
from canary.errors import OrchestratorError
from canary.health import HealthGate
from canary.orchestrator import ReleaseOrchestrator
from canary.store import JsonStateStore

logging.basicConfig(
    level=os.environ.get("CANARY_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("canary.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动加载：不兼容数据会被隔离并告警，不阻止服务启动
    store = JsonStateStore(get_data_path())
    gate = HealthGate(
        failure_threshold=int(os.environ.get("CANARY_FAILURE_THRESHOLD", "3")),
        timeout_seconds=float(os.environ.get("CANARY_PROBE_TIMEOUT", "1.0")),
    )
    app.state.orchestrator = ReleaseOrchestrator(store, gate)
    if store.warnings:
        logger.warning("启动时发现 %d 份不兼容状态数据，已隔离：%s",
                       len(store.warnings), [w["kind"] for w in store.warnings])
    logger.info("发布编排器已就绪，状态文件=%s", store.path)
    yield


app = FastAPI(title="Canary Release Orchestrator", version="0.1.0", lifespan=lifespan)
app.include_router(canary_router)


@app.exception_handler(OrchestratorError)
async def orchestrator_error_handler(request: Request, exc: OrchestratorError) -> JSONResponse:
    # 每类业务异常映射独立 HTTP 状态与 error code，拒绝结果可区分
    logger.info("请求被拒绝/失败 path=%s code=%s message=%s",
                request.url.path, exc.code, exc.message)
    from canary.api import _error_response

    return JSONResponse(status_code=exc.http_status, content=_error_response(exc))


@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError) -> JSONResponse:
    # 跨字段业务规则（如权重严格递增、末项 100、版本不同）在校验阶段失败时，
    # 同样返回结构化、可区分的 422，而不是 500
    safe_details = [
        {
            "loc": list(err.get("loc", [])),
            "type": err.get("type"),
            "msg": err.get("msg"),
            "input": str(err.get("input")),
        }
        for err in exc.errors()
    ]
    logger.info("请求参数被拒绝 path=%s errors=%s",
                request.url.path, [d["msg"] for d in safe_details])
    return JSONResponse(status_code=422, content={
        "error": "validation_error",
        "message": "请求参数不满足发布计划约束",
        "details": safe_details,
    })


@app.get("/")
def read_root():
    return {"Hello": "World"}
