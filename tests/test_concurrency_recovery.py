"""并发安全、重启恢复与状态兼容性测试。"""

from __future__ import annotations

import asyncio
import json

import pytest

from conftest import create_release, register_scripted
from release_orchestrator.health import HealthProbeRegistry
from release_orchestrator.models import (
    HealthProbeResult,
    HealthVerdict,
    ReleasePlan,
)
from release_orchestrator.orchestrator import ReleaseOrchestrator


def test_concurrent_advance_only_one_effective(state_path, registry, clock):
    """同一事件循环内并发推进同一发布：恰好一个有效切换，其余得到 advance_conflict。

    直接压编排器层（共享同一个事件循环），验证 asyncio 每发布锁。
    """
    async def scenario():
        orch = ReleaseOrchestrator.from_path(str(state_path), registry, clock=clock)
        plan = ReleasePlan("p", "v2", "v1", [10, 50, 100], 60)
        rec = orch.create_release(plan)
        register_scripted(registry, rec.id)  # 始终健康

        # 10 个并发首次推进：只能有一个 STARTED
        results = await asyncio.gather(
            *[orch.advance(rec.id) for _ in range(10)],
            return_exceptions=True,
        )
        started = [r for r in results if not isinstance(r, Exception)]
        conflicts = [r for r in results if r.__class__.__name__ == "AdvanceConflictError"]
        assert len(started) == 1, results
        assert len(conflicts) == 9, results
        view = orch.get_release(rec.id)
        assert view.status.value == "in_progress"
        assert view.current_batch == 1
        assert view.current_weight == 10

        # 观察期满后再次并发：同样只有一个真正过门
        clock.advance(61)
        results = await asyncio.gather(
            *[orch.advance(rec.id) for _ in range(10)],
            return_exceptions=True,
        )
        advanced = [
            r for r in results
            if not isinstance(r, Exception) and r[1].value == "advanced"
        ]
        assert len(advanced) == 1, results
        # 门禁探测只能被调用一次（无重复切换）
        assert registry.get(rec.id).call_count == 1
        view = orch.get_release(rec.id)
        assert view.current_batch == 2
        assert view.current_weight == 50
        return rec.id

    asyncio.run(scenario())


def test_restart_resumes_without_repeating_batches(state_path, registry, clock):
    """重启恢复：已完成批次不重复执行，状态/权重/失败计数从磁盘恢复。"""
    orch = ReleaseOrchestrator.from_path(str(state_path), registry, clock=clock)
    plan = ReleasePlan("p", "v2", "v1", [10, 50, 100], 60)
    rec = orch.create_release(plan)
    # 第 1 次门禁不健康（失败计数=1），第 2 次健康
    register_scripted(
        registry, rec.id,
        [HealthProbeResult(HealthVerdict.UNHEALTHY, "blip"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok")],
    )
    asyncio.run(orch.advance(rec.id))  # started
    clock.advance(61)
    with pytest.raises(Exception):
        asyncio.run(orch.advance(rec.id))  # 第一次门禁不健康，计数+1，保持权重

    # 模拟进程重启：新建编排器从同一文件加载，使用全新注册表（探测器重新注入）
    registry2 = HealthProbeRegistry()
    register_scripted(
        registry2, rec.id,
        [HealthProbeResult(HealthVerdict.HEALTHY, "ok"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok")],
    )
    orch2 = ReleaseOrchestrator.from_path(str(state_path), registry2, clock=clock)
    restored = orch2.get_release(rec.id)
    assert restored.status.value == "in_progress"
    assert restored.current_batch == 1
    assert restored.current_weight == 10
    # 连续失败计数跨重启保留
    assert restored.batches[0].consecutive_failures == 1
    assert restored.batches[0].completed is False
    assert restored.events[-1]["type"] == "gate_unhealthy"

    # 恢复后推进：从第 1 批门禁继续，而不是重新启动/跳过
    clock.advance(61)
    _, action = asyncio.run(orch2.advance(rec.id))
    assert action.value == "advanced"
    restored = orch2.get_release(rec.id)
    assert restored.batches[0].completed is True   # 第 1 批只完成一次
    assert restored.current_batch == 2
    assert restored.current_weight == 50

    # 再重启一次，继续走完
    registry3 = HealthProbeRegistry()
    register_scripted(registry3, rec.id)
    orch3 = ReleaseOrchestrator.from_path(str(state_path), registry3, clock=clock)
    assert orch3.get_release(rec.id).completed_batches == 1  # 没有重复累计
    clock.advance(61)
    asyncio.run(orch3.advance(rec.id))
    clock.advance(61)
    _, action = asyncio.run(orch3.advance(rec.id))
    assert action.value == "completed"
    assert orch3.get_release(rec.id).completed_batches == 3


def test_restart_paused_state_recovered(state_path, registry, clock):
    """暂停态跨重启恢复，仍需 resume。"""
    orch = ReleaseOrchestrator.from_path(str(state_path), registry, clock=clock)
    plan = ReleasePlan("p", "v2", "v1", [10, 100], 60)
    rec = orch.create_release(plan)
    register_scripted(
        registry, rec.id,
        [HealthProbeResult(HealthVerdict.DEGRADED, "slow")],
    )
    asyncio.run(orch.advance(rec.id))
    clock.advance(61)
    _, action = asyncio.run(orch.advance(rec.id))
    assert action.value == "paused"

    orch2 = ReleaseOrchestrator.from_path(
        str(state_path), HealthProbeRegistry(), clock=clock
    )
    r2 = orch2.get_release(rec.id)
    assert r2.status.value == "paused"
    assert r2.current_weight == 10
    from release_orchestrator.errors import DuplicateAdvanceError
    with pytest.raises(DuplicateAdvanceError):
        asyncio.run(orch2.advance(rec.id))
    asyncio.run(orch2.resume(rec.id))
    assert orch2.get_release(rec.id).status.value == "in_progress"


def test_rolled_back_state_survives_restart(state_path, registry, clock):
    """回滚后重启：状态、权重、版本仍一致，事件链完整。"""
    orch = ReleaseOrchestrator.from_path(str(state_path), registry, clock=clock)
    plan = ReleasePlan("p", "v2", "v1", [10, 100], 60, max_consecutive_failures=1)
    rec = orch.create_release(plan)
    register_scripted(
        registry, rec.id,
        [HealthProbeResult(HealthVerdict.UNHEALTHY, "fatal")],
    )
    asyncio.run(orch.advance(rec.id))
    clock.advance(61)
    from release_orchestrator.errors import ConsecutiveFailureError
    with pytest.raises(ConsecutiveFailureError):
        asyncio.run(orch.advance(rec.id))

    orch2 = ReleaseOrchestrator.from_path(
        str(state_path), HealthProbeRegistry(), clock=clock
    )
    r2 = orch2.get_release(rec.id)
    assert r2.status.value == "rolled_back"
    assert r2.current_weight == 0
    assert r2.active_version == "v1"
    assert r2.rollback_reason
    rollback_events = [e for e in r2.events if e["type"] == "rollback"]
    assert len(rollback_events) == 1  # 回滚动作只发生一次


def test_corrupt_file_does_not_block_startup(state_path):
    """整个状态文件损坏：备份隔离 + 空库启动，/health 可查。"""
    state_path.write_text("{broken json")
    orch = ReleaseOrchestrator.from_path(str(state_path), HealthProbeRegistry())
    assert orch.list_releases() == []
    assert len(orch.quarantined) == 1
    assert "无法解析" in orch.quarantined[0].reason
    # 原文件被备份
    assert list(state_path.parent.glob("*.quarantine.*"))


def test_incompatible_record_is_marked_not_dropped(state_path, plan_body):
    """单条不兼容记录被显式隔离并持久化，其余记录正常加载。"""
    # 先通过 HTTP 建一条合法发布
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from release_orchestrator.api import router

    def make_client():
        app = FastAPI()
        reg = HealthProbeRegistry()
        app.state.orchestrator = ReleaseOrchestrator.from_path(
            str(state_path), reg, clock=lambda: 1000.0
        )
        app.state.probes = reg
        app.include_router(router)
        return TestClient(app), app

    client, _ = make_client()
    good = create_release(client, plan_body)

    # 手工篡改：植入状态非法的坏记录
    data = json.loads(state_path.read_text())
    data["releases"]["brokenid"] = {
        "id": "brokenid",
        "plan": {
            "name": "bad", "candidate": "x", "stable": "y",
            "weights": [10, 50, 100], "min_observe_seconds": 1,
        },
        "status": "martian-status",
        "current_batch": 0,
        "batches": [
            {"weight": 10}, {"weight": 50}, {"weight": 100},
        ],
        "current_weight": 0,
        "active_version": "y",
        "created_at": 1, "updated_at": 1,
    }
    state_path.write_text(json.dumps(data))

    client2, app2 = make_client()
    orch = app2.state.orchestrator
    # 合法记录仍在
    assert orch.get_release(good["id"]).plan.name == plan_body["name"]
    # 坏记录被显式标记
    q = orch.quarantined
    assert len(q) == 1
    assert q[0].release_id == "brokenid"
    assert "martian-status" in q[0].reason
    # /health 暴露
    resp = client2.get("/health")
    assert resp.json()["quarantined_count"] == 1

    # 再做一次写操作后，隔离标记仍持久化（不被静默吞掉）
    body = dict(plan_body, name="another")
    create_release(client2, body)
    client3, app3 = make_client()
    assert len(app3.state.orchestrator.quarantined) == 1


def test_future_schema_version_quarantined(state_path):
    """更高 schema 版本：整体隔离，空库启动，而不是崩溃。"""
    state_path.write_text(json.dumps({"schema_version": 999, "releases": {}}))
    orch = ReleaseOrchestrator.from_path(str(state_path), HealthProbeRegistry())
    assert orch.list_releases() == []
    assert orch.quarantined[0].release_id == "<whole-state-file>"
    assert "999" in orch.quarantined[0].reason
