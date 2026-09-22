"""健康门禁异常处置：探测超时、连续失败、人工回滚，回滚后状态必须一致可解释。"""

from __future__ import annotations

import pytest

from canary.errors import ConsecutiveFailureError, ProbeTimeoutError, IllegalTransitionError
from canary.models import (
    FailureKind,
    ReleasePlan,
    ReleaseStatus,
)


def plan(rid: str = "rel-h", threshold_batches=None) -> ReleasePlan:
    return ReleasePlan(
        release_id=rid,
        candidate_version="2.0.0",
        stable_version="1.0.0",
        batch_weights=[10, 50, 100],
        min_observation_seconds=0,
    )


def start_batch_zero(orch, rid="rel-h"):
    orch.advance(rid)  # 零观察期：切 10% 后立即门禁


def test_probe_timeout_auto_rollback(orch, gate):
    orch.create_release(plan())
    gate.inject_timeout("rel-h")
    with pytest.raises(ProbeTimeoutError) as ei:
        start_batch_zero(orch)
    assert ei.value.code == "probe_timeout"
    assert ei.value.http_status == 504
    assert ei.value.rolled_back is True

    state = ei.value.state_after
    assert state.status == ReleaseStatus.ROLLED_BACK
    assert state.current_weight == 0
    assert state.active_version == "1.0.0"
    assert state.last_failure == FailureKind.TIMEOUT
    assert "超时" in state.rollback_reason
    print(f"\n探测超时链路 code={ei.value.code} -> 自动回滚: {state.rollback_reason}")
    print("回滚后权重: 候选 0% / 稳定 100%，版本与权重一致")


def test_consecutive_failures_chain(orch, gate):
    rid = "rel-cf"
    orch.create_release(plan(rid))
    gate.inject_unhealthy(rid, "5xx 偏高")
    r = orch.advance(rid)
    assert r.action == "paused"
    print(f"\n失败1/3: {r.gate.reason} -> PAUSED，权重保持 {r.state.current_weight}%")

    orch.resume(rid)
    gate.inject_unhealthy(rid, "p99 延迟超标")
    r = orch.advance(rid)
    assert r.action == "paused"
    print(f"失败2/3: {r.gate.reason} -> PAUSED")

    orch.resume(rid)
    gate.inject_unhealthy(rid, "错误率继续升高")
    with pytest.raises(ConsecutiveFailureError) as ei:
        orch.advance(rid)
    assert ei.value.code == "consecutive_failures"
    assert ei.value.http_status == 502
    assert ei.value.rolled_back is True
    state = ei.value.state_after
    assert state.status == ReleaseStatus.ROLLED_BACK
    assert state.current_weight == 0
    assert state.active_version == "1.0.0"
    assert state.last_failure == FailureKind.CONSECUTIVE_FAILURE
    print(f"失败3/3 -> 链路={ei.value.code}，自动回滚: {state.rollback_reason}")


def test_healthy_resets_failure_count(orch, gate):
    rid = "rel-reset"
    orch.create_release(plan(rid))
    gate.inject_unhealthy(rid, "抖动")
    assert orch.advance(rid).action == "paused"
    orch.resume(rid)
    gate.inject_healthy(rid, "恢复")
    r = orch.advance(rid)
    assert r.action == "batch_completed"
    assert r.gate.consecutive_failures == 0
    print(f"\n一次健康样本清零连续失败计数，批次完成: {r.gate.reason}")


def test_manual_rollback_from_paused(orch, gate):
    rid = "rel-mr"
    orch.create_release(plan(rid))
    gate.inject_unhealthy(rid, "可疑")
    orch.advance(rid)  # paused at 10%
    r = orch.rollback(rid, reason="值班同学决定回退")
    assert r.rolled_back is True
    state = r.state
    assert state.status == ReleaseStatus.ROLLED_BACK
    assert state.current_weight == 0
    assert state.active_version == "1.0.0"
    assert r.weight_split.stable_weight == 100
    print(f"\n人工回滚: {state.rollback_reason}；最终 {r.weight_split.stable_version}=100%")

    # 回滚是终态：再推进 / 再回滚都拒绝，且代码可区分
    with pytest.raises(IllegalTransitionError) as ei:
        orch.advance(rid)
    assert ei.value.code == "illegal_transition"
    with pytest.raises(IllegalTransitionError):
        orch.rollback(rid)
    print("回滚终态再操作被 illegal_transition 拒绝")


def test_real_probe_timeout_path(orch):
    """探针函数真实耗时超过 timeout_seconds 时，同样走超时链路。"""
    from canary.health import HealthGate
    from canary.orchestrator import ReleaseOrchestrator

    slow_gate = HealthGate(timeout_seconds=0.01, probe=lambda rid, v: __import__("time").sleep(0.05) or True)
    o2 = ReleaseOrchestrator(orch.store, slow_gate)
    rid = "rel-slow"
    o2.create_release(plan(rid))
    with pytest.raises(ProbeTimeoutError) as ei:
        o2.advance(rid)
    assert ei.value.state_after.last_failure == FailureKind.TIMEOUT
    print(f"\n真实耗时探针超时 -> {ei.value.message} -> 已回滚")
