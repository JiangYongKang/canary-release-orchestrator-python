"""pytest 公共夹具。

- 每个测试使用独立临时状态文件，互不干扰；
- 注入可控时钟（FakeClock），模拟观察期流逝，无需真实 sleep；
- 默认 INFO 日志在测试过程中打印输入、状态变化与判定依据。
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient

from release_orchestrator.health import HealthProbeRegistry, ScriptedHealthProbe
from release_orchestrator.orchestrator import ReleaseOrchestrator
from release_orchestrator import models as _models  # noqa: F401


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


class FakeClock:
    """可控单调时钟。"""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def state_path(tmp_path: Path) -> Path:
    return tmp_path / "release_state.json"


@pytest.fixture
def registry() -> HealthProbeRegistry:
    return HealthProbeRegistry()


def make_orchestrator(state_path: Path, registry: HealthProbeRegistry, clock: FakeClock) -> ReleaseOrchestrator:
    return ReleaseOrchestrator.from_path(str(state_path), registry, clock=clock)


@pytest.fixture
def orchestrator(state_path, registry, clock) -> ReleaseOrchestrator:
    return make_orchestrator(state_path, registry, clock)


@pytest.fixture
def client(state_path, registry, clock):
    """构造挂载临时编排器的 TestClient（不导入全局 main，避免状态文件串扰）。"""
    from fastapi import FastAPI

    from release_orchestrator.api import router

    app = FastAPI()
    app.state.orchestrator = make_orchestrator(state_path, registry, clock)
    app.state.probes = registry
    app.include_router(router)
    with TestClient(app) as c:
        yield c


@pytest.fixture
def plan_body() -> dict:
    return {
        "name": "checkout-service",
        "candidate": "v2.0.0",
        "stable": "v1.9.0",
        "weights": [10, 50, 100],
        "min_observe_seconds": 60.0,
        "max_consecutive_failures": 3,
    }


def create_release(client, body: dict) -> dict:
    resp = client.post("/releases", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def register_scripted(registry, release_id, verdicts=None, **kwargs):
    probe = ScriptedHealthProbe(verdicts=verdicts, **kwargs)
    registry.register(release_id, probe)
    return probe
