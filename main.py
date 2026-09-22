"""FastAPI 服务入口。

保持原有 GET / 行为兼容（返回 {"Hello": "World"}），
并挂载本地金丝雀发布编排能力。
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI

from release_orchestrator.api import router as release_router
from release_orchestrator.health import HealthProbeRegistry
from release_orchestrator.orchestrator import ReleaseOrchestrator

STATE_FILE = os.environ.get(
    "RELEASE_STATE_FILE", str(Path(__file__).parent / ".release_state.json")
)

app = FastAPI(title="Local Canary Release Orchestrator")

# 进程内单例：启动时从本地文件恢复状态；历史脏数据不会阻止启动。
probes = HealthProbeRegistry()
orchestrator = ReleaseOrchestrator.from_path(STATE_FILE, probes)
app.state.orchestrator = orchestrator
app.state.probes = probes

app.include_router(release_router)


@app.get("/")
def read_root():
    return {"Hello": "World"}
