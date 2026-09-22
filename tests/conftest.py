"""pytest 公共夹具：

- 每个测试使用独立临时状态文件；
- 打开日志，测试运行时打印输入、状态变化与判定依据（caplog / logging）。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from canary.health import HealthGate
from canary.orchestrator import ReleaseOrchestrator
from canary.store import JsonStateStore

# 让“输入、状态变化、判定依据”在测试输出中可见
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    force=True,
)


@pytest.fixture()
def state_path(tmp_path: Path) -> Path:
    return tmp_path / "data" / "releases.json"


@pytest.fixture()
def store(state_path: Path) -> JsonStateStore:
    return JsonStateStore(state_path)


@pytest.fixture()
def gate() -> HealthGate:
    return HealthGate(failure_threshold=3, timeout_seconds=1.0)


@pytest.fixture()
def orch(store: JsonStateStore, gate: HealthGate) -> ReleaseOrchestrator:
    return ReleaseOrchestrator(store, gate)
