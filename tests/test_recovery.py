"""重启恢复与状态兼容。

- 重启后从持久化状态继续：已完成批次不重复执行，进度/权重/状态延续；
- 损坏 JSON、过新 schema、单条坏记录都不得阻止服务启动；
- 不兼容数据必须被明确标记（quarantine + compat 报告），不静默忽略。
"""

from __future__ import annotations

import json
from pathlib import Path

from canary.health import HealthGate
from canary.models import ReleasePlan, ReleaseStatus, SCHEMA_VERSION
from canary.orchestrator import ReleaseOrchestrator
from canary.store import JsonStateStore


def test_resume_after_restart_skips_completed_batches(state_path: Path):
    rid = "rel-restart"
    store = JsonStateStore(state_path)
    gate = HealthGate(failure_threshold=3)
    orch = ReleaseOrchestrator(store, gate)
    orch.create_release(ReleasePlan(
        release_id=rid, candidate_version="2.0.0", stable_version="1.0.0",
        batch_weights=[30, 70, 100], min_observation_seconds=0,
    ))
    gate.inject_healthy(rid)
    orch.advance(rid)  # batch0 完成
    gate.inject_unhealthy(rid, "波动")
    orch.advance(rid)  # batch1 暂停在 70%
    assert orch.get(rid).completed_batches == 1
    assert orch.get(rid).current_weight == 70
    print(f"\n重启前: completed=1 状态=PAUSED 候选权重=70%")

    # ---- 模拟进程重启：全新 store / gate / orchestrator，指向同一状态文件 ----
    store2 = JsonStateStore(state_path)
    gate2 = HealthGate(failure_threshold=3)
    orch2 = ReleaseOrchestrator(store2, gate2)
    state = orch2.get(rid)
    assert state.completed_batches == 1
    assert state.status == ReleaseStatus.PAUSED
    assert state.current_weight == 70  # 暂停时的权重被持久化
    print(f"重启后: completed=1 状态={state.status.value} 候选权重={state.current_weight}%（无重复执行批次0）")

    gate2.inject_healthy(rid, "恢复稳定")
    r = orch2.resume(rid)
    r = orch2.advance(rid)  # 直接从批次1继续
    assert state.completed_batches in (1, 2)  # 断言取下边最终值
    assert r.state.completed_batches == 2
    print(f"恢复推进: 批次1 完成 completed=2，未重复批次0")


def test_restart_after_completion_is_terminal(state_path: Path):
    rid = "rel-done"
    store = JsonStateStore(state_path)
    gate = HealthGate()
    orch = ReleaseOrchestrator(store, gate)
    orch.create_release(ReleasePlan(
        release_id=rid, candidate_version="2.0.0", stable_version="1.0.0",
        batch_weights=[100], min_observation_seconds=0,
    ))
    gate.inject_healthy(rid)
    orch.advance(rid)

    store2 = JsonStateStore(state_path)
    orch2 = ReleaseOrchestrator(store2, HealthGate())
    state = orch2.get(rid)
    assert state.status == ReleaseStatus.COMPLETED
    assert state.active_version == "2.0.0"
    print(f"\n重启后 COMPLETED 终态保持: active={state.active_version} weight=100%")


def test_corrupt_json_does_not_block_startup(state_path: Path):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{ this is not json ", encoding="utf-8")
    store = JsonStateStore(state_path)
    assert store.load_all() == {}
    kinds = [w["kind"] for w in store.warnings]
    assert "corrupt_json" in kinds
    qdir = state_path.parent / "quarantine"
    assert any(qdir.glob("*.json"))
    print(f"\n损坏 JSON 不阻止启动；已隔离并标记: {kinds}")
    # 被隔离后新写入可正常工作
    orch = ReleaseOrchestrator(store, HealthGate())
    orch.create_release(ReleasePlan(
        release_id="fresh", candidate_version="2", stable_version="1",
        batch_weights=[100], min_observation_seconds=0,
    ))
    assert orch.get("fresh").status == ReleaseStatus.PENDING


def test_newer_schema_is_quarantined(state_path: Path):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({
        "schema_version": SCHEMA_VERSION + 99,
        "releases": {"x": {"release_id": "x"}},
    }), encoding="utf-8")
    store = JsonStateStore(state_path)
    assert store.load_all() == {}
    assert any(w["kind"] == "incompatible_schema" for w in store.warnings)
    report = store.compat_report()
    assert report["schema_version"] == SCHEMA_VERSION and report["warnings"]
    print(f"\n更新版本 schema 拒绝猜测解析，整体隔离: "
          f"{[w['reason'] for w in report['warnings'] if w['kind']=='incompatible_schema']}")


def test_one_bad_record_is_isolated_others_load(state_path: Path):
    good_store = JsonStateStore(state_path)
    o = ReleaseOrchestrator(good_store, HealthGate())
    o.create_release(ReleasePlan(
        release_id="good", candidate_version="2", stable_version="1",
        batch_weights=[100], min_observation_seconds=0,
    ))
    # 手工塞入一条无法校验的记录（status 非法），同时保留 good
    data = json.loads(state_path.read_text(encoding="utf-8"))
    data["releases"]["bad"] = {"release_id": "bad", "status": "mystery_state"}
    state_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    store = JsonStateStore(state_path)
    states = store.load_all()
    assert "good" in states and "bad" not in states
    assert any(w["kind"] == "invalid_release_record" for w in store.warnings)
    print(f"\n单条坏记录隔离: good 正常加载，bad 标记 -> "
          f"{[w['kind'] for w in store.warnings]}")
