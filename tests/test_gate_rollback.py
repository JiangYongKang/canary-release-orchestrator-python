"""健康门禁、异常处置与回滚测试。"""

from __future__ import annotations

from conftest import create_release, register_scripted
from release_orchestrator.models import HealthProbeResult, HealthVerdict


def _u(reason: str = "error rate 15%") -> HealthProbeResult:
    return HealthProbeResult(HealthVerdict.UNHEALTHY, reason)


def _d(reason: str = "latency p99 rising") -> HealthProbeResult:
    return HealthProbeResult(HealthVerdict.DEGRADED, reason)


def _h(reason: str = "ok") -> HealthProbeResult:
    return HealthProbeResult(HealthVerdict.HEALTHY, reason)


def _start(client, registry, rid):
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200
    return r.json()


def test_consecutive_failures_trigger_rollback(client, clock, registry, plan_body):
    """连续不健康达到阈值 -> 独立错误码 + 自动回滚到稳定版本、权重 0。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    # 前两次不健康（未达阈值 3），第三次不健康触发回滚
    register_scripted(registry, rid, [_u(), _u(), _u()])
    _start(client, registry, rid)

    clock.advance(61)
    r1 = client.post(f"/releases/{rid}/advance")
    assert r1.status_code == 409
    assert r1.json()["error_code"] == "duplicate_advance"  # 未达阈值：保持权重
    assert client.get(f"/releases/{rid}").json()["current_weight"] == 10

    clock.advance(61)
    r2 = client.post(f"/releases/{rid}/advance")
    assert r2.status_code == 409
    assert r2.json()["error_code"] == "duplicate_advance"

    clock.advance(61)
    r3 = client.post(f"/releases/{rid}/advance")
    # 达到阈值：可识别异常链路 + 已回滚
    assert r3.status_code == 502
    body = r3.json()
    assert body["error_code"] == "consecutive_failure"
    assert body["action"] == "rolled_back"

    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "rolled_back"
    assert view["current_weight"] == 0            # 候选流量清零
    assert view["active_version"] == "v1.9.0"      # 回到稳定版本
    assert view["rollback_reason"]
    # 回滚后推进 -> 非法转换
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409
    assert r.json()["error_code"] == "illegal_transition"

    events = client.get(f"/releases/{rid}/events").json()["events"]
    assert any(e["type"] == "rollback" for e in events)
    assert any(e["type"] == "gate_threshold_exceeded" for e in events)


def test_recovery_after_single_unhealthy(client, clock, registry, plan_body):
    """未达阈值的单次失败后，下一次探测恢复健康即可继续推进。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(registry, rid, [_u(), _h()])
    _start(client, registry, rid)

    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409 and r.json()["error_code"] == "duplicate_advance"
    view = client.get(f"/releases/{rid}").json()
    assert view["batches"][0]["consecutive_failures"] == 1

    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200
    assert r.json()["action"] == "advanced"
    # 健康后连续失败计数清零
    assert client.get(f"/releases/{rid}").json()["batches"][0]["consecutive_failures"] == 0


def test_probe_timeout_chain(client, clock, registry, plan_body):
    """探测超时 -> probe_timeout 独立异常链路（504），且自动回滚。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    # 首次门禁（即第 1 次 probe 调用）即超时
    register_scripted(registry, rid, timeout_at=1)
    _start(client, registry, rid)

    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 504
    assert r.json()["error_code"] == "probe_timeout"
    assert r.json()["action"] == "rolled_back"

    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "rolled_back"
    assert view["active_version"] == "v1.9.0"
    assert view["current_weight"] == 0
    events = client.get(f"/releases/{rid}/events").json()["events"]
    assert any(e["type"] == "gate_probe_timeout" for e in events)


def test_degraded_pauses_and_resume(client, clock, registry, plan_body):
    """降级判定 -> 暂停并保持权重；resume 后重新观察并可继续。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(registry, rid, [_d(), _h(), _h(), _h(), _h(), _h()])
    _start(client, registry, rid)

    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200
    assert r.json()["action"] == "paused"
    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "paused"
    assert view["current_weight"] == 10  # 暂停时保持当前权重

    # 暂停态直接推进 -> 可区分拒绝
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409
    assert r.json()["error_code"] == "duplicate_advance"

    # 恢复
    r = client.post(f"/releases/{rid}/resume")
    assert r.status_code == 200
    assert r.json()["status"] == "in_progress"
    # resume 立即推进 -> 观察期被重置，仍需等待
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409
    assert r.json()["error_code"] == "duplicate_advance"

    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200
    assert r.json()["action"] == "advanced"
    assert r.json()["release"]["current_weight"] == 50


def test_resume_illegal_states(client, clock, registry, plan_body):
    """非暂停态 resume -> illegal_transition；终态/中止态同理。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(registry, rid)
    # PENDING 态 resume
    r = client.post(f"/releases/{rid}/resume")
    assert r.status_code == 409
    assert r.json()["error_code"] == "illegal_transition"


def test_manual_abort_freezes_release(client, registry, plan_body):
    """人工中止：冻结流量、独立 manual_abort 链路，不自动回滚。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    _start(client, registry, rid)  # 候选 10%

    r = client.post(f"/releases/{rid}/abort", json={"reason": "stop by oncall"})
    assert r.status_code == 200
    view = r.json()
    assert view["status"] == "aborted"
    assert view["current_weight"] == 10          # 冻结，不回滚
    assert view["rollback_reason"] == "stop by oncall"

    # 对已中止发布推进/恢复 -> manual_abort 独立链路
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409
    assert r.json()["error_code"] == "manual_abort"
    r = client.post(f"/releases/{rid}/resume")
    assert r.status_code == 409
    assert r.json()["error_code"] == "manual_abort"

    # 重复中止 -> duplicate_advance
    r = client.post(f"/releases/{rid}/abort", json={"reason": "again"})
    assert r.status_code == 409
    assert r.json()["error_code"] == "duplicate_advance"


def test_abort_terminal_rejected(client, clock, registry, plan_body):
    """已完成的发布不允许中止。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(registry, rid)
    client.post(f"/releases/{rid}/advance")
    for _ in range(3):
        clock.advance(61)
        client.post(f"/releases/{rid}/advance")
    assert client.get(f"/releases/{rid}").json()["status"] == "completed"
    r = client.post(f"/releases/{rid}/abort", json={"reason": "x"})
    assert r.status_code == 409
    assert r.json()["error_code"] == "illegal_transition"
