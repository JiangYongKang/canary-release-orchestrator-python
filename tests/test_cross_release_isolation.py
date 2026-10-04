"""跨发布隔离：某条发布的探测变慢/卡住时，不相关发布的操作不受影响。

复现的场景：多实例共用同一份状态文件时，一条发布的健康门禁探测耗时
数秒（依赖的内部指标接口慢），另一条完全不相关的发布的推进/恢复/中止
不应被拖着等待，更不应得到 state_commit_busy 一类的"状态提交繁忙"失败；
只有同时改动同一条发布的调用才允许短暂互斥（且是非阻塞的单赢家语义）。

同时验证：同一条发布在探测卡住期间被并发推进时，仍然只有一个生效。
"""

from __future__ import annotations

import asyncio
import threading
import time

from conftest import register_scripted
from release_orchestrator.errors import AdvanceConflictError
from release_orchestrator.health import AlwaysHealthyProbe, HealthProbeRegistry
from release_orchestrator.models import (
    GateAction,
    HealthProbeResult,
    HealthVerdict,
    ReleasePlan,
    ReleaseStatus,
)
from release_orchestrator.orchestrator import ReleaseOrchestrator


class BlockingProbe:
    """进入探测时发信号，然后阻塞直到测试放行（模拟依赖内部指标的慢探测）。"""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def probe(self, release_id, candidate, weight):
        self.calls += 1
        self.entered.set()
        assert self.release.wait(timeout=15), "测试未放行探测"
        return HealthProbeResult(HealthVerdict.HEALTHY, "slow but healthy")


def _run(coro):
    return asyncio.run(coro)


def _make_orch(state_path, clock, registry) -> ReleaseOrchestrator:
    # commit_timeout 调小：若慢探测仍占用全局提交锁，无关发布的操作必然
    # 等满 0.5s 并以 state_commit_busy 失败（即修复前的行为），测试会立刻暴露。
    return ReleaseOrchestrator.from_path(
        str(state_path), registry, clock=clock, commit_timeout=0.5
    )


def _advance_in_thread(orch, rid, outcome, key="result"):
    def _target():
        try:
            outcome[key] = _run(orch.advance(rid))
        except Exception as exc:  # noqa: BLE001 - 回传给主线程断言
            outcome["error"] = exc

    t = threading.Thread(target=_target)
    t.start()
    return t


def test_slow_probe_does_not_block_unrelated_release(state_path, clock):
    """A 的门禁探测卡住时，无关发布 B 的推进在合理时间内生效。"""
    registry = HealthProbeRegistry()
    orch = _make_orch(state_path, clock, registry)
    a = orch.create_release(ReleasePlan("svc-a", "v2", "v1", [100], 0.0))
    b = orch.create_release(ReleasePlan("svc-b", "v2", "v1", [100], 0.0))
    probe = BlockingProbe()
    registry.register(a.id, probe)
    registry.register(b.id, AlwaysHealthyProbe())

    _run(orch.advance(a.id))  # A 启动首批（不触发探测）

    outcome: dict = {}
    t = _advance_in_thread(orch, a.id, outcome)  # A 第二次推进：进入门禁，探测卡住
    assert probe.entered.wait(timeout=5), "A 的探测未进入"

    t0 = time.monotonic()
    _, act1 = _run(orch.advance(b.id))  # B started
    _, act2 = _run(orch.advance(b.id))  # B completed（观察期 0）
    elapsed = time.monotonic() - t0

    assert (act1, act2) == (GateAction.STARTED, GateAction.COMPLETED)
    assert elapsed < 2.0, f"无关发布被慢探测拖住: {elapsed:.2f}s"
    view_b = orch.get_release(b.id)
    assert view_b.status is ReleaseStatus.COMPLETED
    assert view_b.current_weight == 100

    probe.release.set()
    t.join(timeout=15)
    assert "error" not in outcome, outcome.get("error")
    _, act_a = outcome["result"]
    assert act_a == GateAction.COMPLETED
    print(f"[判定] A 探测卡住期间，B 推进到 completed 仅耗时 {elapsed:.3f}s；"
          f"A 放行后也正常 completed")


def test_hung_probe_unrelated_abort_and_resume_succeed(state_path, clock):
    """A 探测卡住时，无关发布的中止与（暂停态的）恢复都立即生效。"""
    registry = HealthProbeRegistry()
    orch = _make_orch(state_path, clock, registry)
    a = orch.create_release(ReleasePlan("svc-a", "v2", "v1", [100], 0.0))
    b = orch.create_release(ReleasePlan("svc-b", "v2", "v1", [100], 0.0))
    c = orch.create_release(ReleasePlan("svc-c", "v2", "v1", [100], 0.0))
    probe = BlockingProbe()
    registry.register(a.id, probe)
    registry.register(b.id, AlwaysHealthyProbe())
    register_scripted(
        registry, c.id, [HealthProbeResult(HealthVerdict.DEGRADED, "slow")]
    )

    _run(orch.advance(a.id))  # A started
    _run(orch.advance(b.id))  # B started
    _run(orch.advance(c.id))  # C started
    _, act = _run(orch.advance(c.id))  # C 门禁降级 -> paused
    assert act == GateAction.PAUSED

    outcome: dict = {}
    t = _advance_in_thread(orch, a.id, outcome)  # A 进入门禁，探测卡住
    assert probe.entered.wait(timeout=5), "A 的探测未进入"

    t0 = time.monotonic()
    _run(orch.abort(b.id, "emergency stop"))  # 无关发布中止：立即生效
    _run(orch.resume(c.id))                   # 无关暂停发布恢复：立即生效
    elapsed = time.monotonic() - t0

    assert elapsed < 2.0, f"无关发布操作被慢探测拖住: {elapsed:.2f}s"
    assert orch.get_release(b.id).status is ReleaseStatus.ABORTED
    assert orch.get_release(b.id).current_weight == 100  # 中止冻结当前权重
    assert orch.get_release(c.id).status is ReleaseStatus.IN_PROGRESS

    probe.release.set()
    t.join(timeout=15)
    assert "error" not in outcome, outcome.get("error")
    print(f"[判定] A 探测卡住期间，B abort + C resume 共耗时 {elapsed:.3f}s，"
          f"均立即生效")


def test_same_release_slow_probe_still_single_winner(state_path, clock):
    """同一条发布在探测卡住期间被并发推进：仍只有一个生效，探测不重复。"""
    registry = HealthProbeRegistry()
    orch = _make_orch(state_path, clock, registry)
    a = orch.create_release(ReleasePlan("svc-a", "v2", "v1", [100], 0.0))
    probe = BlockingProbe()
    registry.register(a.id, probe)
    _run(orch.advance(a.id))  # started -> v3

    outcome: dict = {}
    t1 = _advance_in_thread(orch, a.id, outcome)  # 进入门禁，探测卡住（持发布锁）
    assert probe.entered.wait(timeout=5), "A 的探测未进入"

    # 探测卡住期间，另外两个并发推进同一发布：必须立即以可区分方式未生效
    conflicts: list[Exception] = []

    def hit():
        try:
            _run(orch.advance(a.id))
        except AdvanceConflictError as exc:
            conflicts.append(exc)

    t2, t3 = threading.Thread(target=hit), threading.Thread(target=hit)
    t0 = time.monotonic()
    t2.start(); t3.start(); t2.join(timeout=10); t3.join(timeout=10)
    elapsed = time.monotonic() - t0

    assert len(conflicts) == 2, "并发调用应立即以 advance_conflict 未生效返回"
    assert elapsed < 2.0, f"同发布并发冲突的返回也被拖住: {elapsed:.2f}s"
    assert probe.calls == 1  # 慢探测没有被并发调用重复触发

    probe.release.set()
    t1.join(timeout=15)
    assert "error" not in outcome, outcome.get("error")
    _, act = outcome["result"]
    assert act == GateAction.COMPLETED

    final = orch.get_release(a.id)
    assert final.status is ReleaseStatus.COMPLETED
    assert final.batches[0].gate_attempts == 1  # 门禁只执行一次
    assert sum(1 for e in final.events if e["type"] == "release_completed") == 1
    assert final.version == 4  # create/started/completed；冲突调用不留痕
    print(f"[判定] 探测卡住期间 2 个并发调用 {elapsed:.3f}s 内以 advance_conflict "
          f"返回，门禁只执行一次，最终 version=v{final.version}")
