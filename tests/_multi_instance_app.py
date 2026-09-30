"""多实例 HTTP 测试专用 FastAPI 入口（与 main.py 同构，状态文件/实例标识由环境变量指定）。"""

from __future__ import annotations

import os

from fastapi import FastAPI

from release_orchestrator.api import router as release_router
from release_orchestrator.health import HealthProbeRegistry
from release_orchestrator.orchestrator import ReleaseOrchestrator

STATE_FILE = os.environ["RELEASE_STATE_FILE"]
INSTANCE_ID = os.environ.get("RELEASE_INSTANCE_ID")

app = FastAPI(title="Multi-instance test node")
probes = HealthProbeRegistry()
app.state.orchestrator = ReleaseOrchestrator.from_path(
    STATE_FILE, probes, instance_id=INSTANCE_ID
)
app.state.probes = probes
app.include_router(release_router)


@app.get("/")
def read_root():
    return {"Hello": "World"}
