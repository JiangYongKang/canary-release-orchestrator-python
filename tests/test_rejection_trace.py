"""终态/暂停态拒绝调用的可追溯性。

对已完成、已回滚、已中止或暂停等待中的发布再次推进/恢复/中止时
（调用方未显式携带 expected_version）：
- 调用仍以原有可区分错误码被拒绝（illegal_transition / manual_abort /
  duplicate_advance），且不改变发布的状态、当前权重与已完成批次；
- 但拒绝本身会写入 operation_rejected 事件（含当时状态、批次与判定依据），
  并让该发布的 version 前进——查询结果与事件记录里都能追溯这次拒绝。
"""

from __future__ import annotations

from conftest import create_release, register_scripted
from release_orchestrator.models import HealthProbeResult, HealthVerdict


def _rejections(events: list[dict]) -> list[dict]:
    return [e for e in events if e["type"] == "operation_rejected"]


def _complete_release(client, clock, registry, plan_body) -> str:
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(registry, rid)  # 始终健康
    client.post(f"/releases/{rid}/advance")
    for _ in range(3):
        clock.advance(61)
        client.post(f"/releases/{rid}/advance")
    assert client.get(f"/releases/{rid}").json()["status"] == "completed"
    return rid


def test_completed_release_rejections_leave_trace(client, clock, registry, plan_body):
    """已完成发布：advance/abort/resume 均被拒且逐次留痕，version 前进。"""
    rid = _complete_release(client, clock, registry, plan_body)
    before = client.get(f"/releases/{rid}").json()
    v0 = before["version"]

    # 未携带 expected_version 的三种调用全部被拒
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409 and r.json()["error_code"] == "illegal_transition"
    r = client.post(f"/releases/{rid}/abort", json={"reason": "too late"})
    assert r.status_code == 409 and r.json()["error_code"] == "illegal_transition"
    r = client.post(f"/releases/{rid}/resume")
    assert r.status_code == 409 and r.json()["error_code"] == "illegal_transition"

    # 拒绝不改变实际状态，但 version 随每次留痕前进
    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "completed"
    assert view["current_weight"] == 100
    assert view["completed_batches"] == 3
    assert view["version"] == v0 + 3

    events = client.get(f"/releases/{rid}/events").json()["events"]
    rej = _rejections(events)
    assert len(rej) == 3
    assert [e["detail"]["op"] for e in rej] == ["advance", "abort", "resume"]
    for e in rej:
        assert e["detail"]["reason"] == "terminal_status"
        assert e["detail"]["current_status"] == "completed"
        assert e["detail"]["current_batch"] == 3
        assert e["detail"]["decision"]
    print(f"[判定] 终态三次拒绝均留痕 operation_rejected，version v{v0} -> v{v0 + 3}")


def test_aborted_release_rejections_leave_trace(client, clock, registry, plan_body):
    """已中止发布：推进/恢复/重复中止被拒并留痕，权重保持冻结。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(registry, rid)
    client.post(f"/releases/{rid}/advance")  # started，候选 10%
    r = client.post(f"/releases/{rid}/abort", json={"reason": "stop"})
    assert r.status_code == 200
    v0 = client.get(f"/releases/{rid}").json()["version"]

    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409 and r.json()["error_code"] == "manual_abort"
    r = client.post(f"/releases/{rid}/resume")
    assert r.status_code == 409 and r.json()["error_code"] == "manual_abort"
    r = client.post(f"/releases/{rid}/abort", json={"reason": "again"})
    assert r.status_code == 409 and r.json()["error_code"] == "duplicate_advance"

    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "aborted"
    assert view["current_weight"] == 10  # 冻结权重不变
    assert view["version"] == v0 + 3

    events = client.get(f"/releases/{rid}/events").json()["events"]
    rej = _rejections(events)
    assert len(rej) == 3
    assert [e["detail"]["op"] for e in rej] == ["advance", "resume", "abort"]
    assert all(e["detail"]["reason"] == "already_aborted" for e in rej)
    assert all(e["detail"]["current_status"] == "aborted" for e in rej)
    # manual_abort 事件仍只有一次（拒绝不会重复中止）
    assert sum(1 for e in events if e["type"] == "manual_abort") == 1
    print(f"[判定] 中止态三次拒绝均留痕，version v{v0} -> v{v0 + 3}，权重仍 10%")


def test_rolled_back_release_rejection_leaves_trace(client, clock, registry, plan_body):
    """已回滚发布：推进被拒并留痕，权重/版本保持回滚后状态。"""
    body = dict(plan_body, max_consecutive_failures=1)
    rel = create_release(client, body)
    rid = rel["id"]
    register_scripted(
        registry, rid, [HealthProbeResult(HealthVerdict.UNHEALTHY, "fatal")]
    )
    client.post(f"/releases/{rid}/advance")
    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 502  # consecutive_failure -> 已回滚
    v0 = client.get(f"/releases/{rid}").json()["version"]

    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409 and r.json()["error_code"] == "illegal_transition"

    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "rolled_back"
    assert view["current_weight"] == 0
    assert view["active_version"] == plan_body["stable"]
    assert view["version"] == v0 + 1

    events = client.get(f"/releases/{rid}/events").json()["events"]
    rej = _rejections(events)
    assert len(rej) == 1
    assert rej[0]["detail"]["reason"] == "terminal_status"
    assert rej[0]["detail"]["current_status"] == "rolled_back"
    assert sum(1 for e in events if e["type"] == "rollback") == 1  # 回滚仍唯一
    print(f"[判定] 回滚终态拒绝留痕，version v{v0} -> v{v0 + 1}，权重保持 0")


def test_paused_release_advance_rejection_leaves_trace(client, clock, registry, plan_body):
    """暂停等待中的发布：直接推进被拒并留痕，随后 resume 仍可正常恢复。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(
        registry, rid,
        [HealthProbeResult(HealthVerdict.DEGRADED, "slow"),
         HealthProbeResult(HealthVerdict.HEALTHY, "ok")],
    )
    client.post(f"/releases/{rid}/advance")
    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.json()["action"] == "paused"
    v0 = client.get(f"/releases/{rid}").json()["version"]

    # 暂停态直接推进（未带 expected_version）-> 拒绝 + 留痕
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409 and r.json()["error_code"] == "duplicate_advance"

    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "paused"
    assert view["current_weight"] == 10
    assert view["version"] == v0 + 1

    events = client.get(f"/releases/{rid}/events").json()["events"]
    rej = _rejections(events)
    assert len(rej) == 1
    assert rej[0]["detail"]["reason"] == "paused_waiting_resume"
    assert rej[0]["detail"]["current_status"] == "paused"
    assert rej[0]["detail"]["current_batch"] == 1

    # 留痕不影响后续正常恢复与推进
    r = client.post(f"/releases/{rid}/resume")
    assert r.status_code == 200 and r.json()["status"] == "in_progress"
    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200 and r.json()["action"] == "advanced"
    print(f"[判定] 暂停态拒绝留痕（version v{v0} -> v{v0 + 1}），随后 resume 正常恢复")


def test_resume_on_non_paused_leaves_trace(client, clock, registry, plan_body):
    """非暂停态（pending/in_progress）resume：拒绝并留痕，状态不变。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(registry, rid)
    v0 = client.get(f"/releases/{rid}").json()["version"]

    r = client.post(f"/releases/{rid}/resume")  # pending 态 resume
    assert r.status_code == 409 and r.json()["error_code"] == "illegal_transition"
    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "pending"
    assert view["version"] == v0 + 1

    events = client.get(f"/releases/{rid}/events").json()["events"]
    rej = _rejections(events)
    assert len(rej) == 1
    assert rej[0]["detail"]["reason"] == "not_paused"
    assert rej[0]["detail"]["current_status"] == "pending"
    print(f"[判定] pending 态 resume 拒绝留痕，version v{v0} -> v{v0 + 1}")
