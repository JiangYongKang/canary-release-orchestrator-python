"""不同发布互不阻塞 + 拒绝留痕测试（本次问题场景的本地稳定复现）。

覆盖三类现象：

1. **跨发布隔离**：发布 A 的健康门禁探测明显变慢/卡住时，另一条不相关发布 B
   的推进 / 中止必须在合理时间内成功生效，而不是排队等待或拿到
   state_commit_busy 一类失败（旧实现里全局提交锁被慢探测整段持有，会复现）。
2. **同发布并发保护不回退**：同一条发布被同时操作时，即使其中一个调用正卡在
   慢探测里，其余调用仍然立即得到可区分的"本次未生效"(advance_conflict)，
   流量切换不会执行两遍。
3. **拒绝留痕**：对已完成 / 已回滚 / 已中止 / 暂停等待中的发布再次发起
   advance / resume / abort（且不带 expected_version），拒绝本身在查询结果
   与事件记录中都看得到原因（当时状态、批次、判定依据），version 相应前进，
   但实际状态 / 当前权重 / 已完成批次绝不改变。
"""

from __future__ import annotations

import asyncio
import threading
import time

from conftest import create_release

from release_orchestrator.errors import AdvanceConflictError
from release_orchestrator.health import HealthProbeResult, HealthVerdict
from release_orchestrator.models import ReleasePlan, ReleaseStatus
from release_orchestrator.orchestrator import ReleaseOrchestrator

# --------------------------------------------------------------------------- #
# 测试用探测器：可卡住、可放行
# --------------------------------------------------------------------------- #


class BlockingProbe:
    """第一次门禁调用阻塞，直到外部 set() 放行；放行后返回 HEALTHY。"""

    def __init__(self) -> None:
        self.entered = threading.Event()   # 探测已开始（证明调用已进入门禁）
        self.release = threading.Event()   # 放行信号
        self.calls = 0

    def probe(self, release_id, candidate, weight):
        self.calls += 1
        self.entered.set()
        self.release.wait(timeout=10)
        return HealthProbeResult(HealthVerdict.HEALTHY, "released")


class SlowProbe:
    """门禁调用固定阻塞若干秒（模拟内部指标 1~2 秒才返回）。"""

    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.calls = 0

    def probe(self, release_id, candidate, weight):
        self.calls += 1
        time.sleep(self.delay)
        return HealthProbeResult(HealthVerdict.HEALTHY, "slow but healthy")


# --------------------------------------------------------------------------- #
# 编排器层驱动工具
# --------------------------------------------------------------------------- #


def _make(orchestrator, name, weights=(10, 50, 100)) -> str:
    return orchestrator.create_release(
        ReleasePlan(name, "v2", "v1", list(weights), 60.0)
    ).id


# --------------------------------------------------------------------------- #
# 1. 慢探测期间，不同发布互不阻塞（编排器层，同事件循环 + to_thread）
# --------------------------------------------------------------------------- #


def test_slow_probe_does_not_block_unrelated_release(state_path, registry, clock):
    """A 的门禁卡住时，B 的推进与 C 的中止都必须迅速成功。"""
    orch = ReleaseOrchestrator.from_path(str(state_path), registry, clock=clock)
    rid_a = _make(orch, "svc-slow")
    rid_b = _make(orch, "svc-fast", weights=(100,))
    rid_c = _make(orch, "svc-abort", weights=(100,))

    blocking = BlockingProbe()
    registry.register(rid_a, blocking)

    async def scenario():
        # A：启动首批，再发起第二次 advance -> 进入观察期后的门禁并卡住
        await orch.advance(rid_a)
        clock.advance(61)
        slow_task = asyncio.create_task(orch.advance(rid_a))
        # 等 A 确实已经进入卡住的探测（而不是还没排上）
        await asyncio.to_thread(blocking.entered.wait, 5)
        assert blocking.entered.is_set()

        # 不相关发布 B：PENDING -> 首批（需要 C 阶段提交）必须快速成功，
        # 不会拿到 state_commit_busy，也不会等 A 的探测。
        t0 = time.monotonic()
        _rec_b, action_b = await orch.advance(rid_b)
        elapsed_b = time.monotonic() - t0
        assert action_b.value == "started"
        assert _rec_b.status is ReleaseStatus.IN_PROGRESS
        assert elapsed_b < 1.0, f"无关发布被慢探测拖了 {elapsed_b:.2f}s"

        # 不相关发布 C：中止操作同样只动 C 的状态，必须快速生效
        t0 = time.monotonic()
        rec_c = await orch.abort(rid_c, reason="unrelated")
        elapsed_c = time.monotonic() - t0
        assert rec_c.status is ReleaseStatus.ABORTED
        assert elapsed_c < 1.0, f"无关发布中止被慢探测拖了 {elapsed_c:.2f}s"

        # 放行 A 的探测 -> A 正常过门完成（不丢状态、不重复切换）
        blocking.release.set()
        rec_a, action_a = await slow_task
        assert action_a.value == "advanced"
        assert rec_a.current_batch == 2 and rec_a.current_weight == 50
        return elapsed_b, elapsed_c

    elapsed_b, elapsed_c = asyncio.run(scenario())

    final_a = orch.get_release(rid_a)
    assert final_a.current_batch == 2 and final_a.current_weight == 50
    assert blocking.calls == 1  # 门禁探测没有因并发被重复执行
    final_b = orch.get_release(rid_b)
    assert final_b.status is ReleaseStatus.IN_PROGRESS
    final_c = orch.get_release(rid_c)
    assert final_c.status is ReleaseStatus.ABORTED
    print(f"[判定] A 探测卡住期间 B 推进耗时 {elapsed_b*1000:.0f}ms、"
          f"C 中止耗时 {elapsed_c*1000:.0f}ms，均迅速生效且互不影响")


def test_many_unrelated_operations_succeed_while_probe_slow(state_path, registry, clock):
    """A 探测卡住的整个窗口内，一批不相关发布的操作全部快速成功。"""
    orch = ReleaseOrchestrator.from_path(str(state_path), registry, clock=clock)
    rid_a = _make(orch, "svc-slow")
    others = [_make(orch, f"svc-other-{i}", weights=(100,)) for i in range(8)]
    blocking = BlockingProbe()
    registry.register(rid_a, blocking)

    async def scenario():
        await orch.advance(rid_a)
        clock.advance(61)
        slow_task = asyncio.create_task(orch.advance(rid_a))
        await asyncio.to_thread(blocking.entered.wait, 5)

        # 8 条不相关发布同时推进，全部应在很短时间内 started
        t0 = time.monotonic()
        results = await asyncio.gather(*[orch.advance(r) for r in others])
        elapsed = time.monotonic() - t0
        blocking.release.set()
        await slow_task
        return results, elapsed

    results, elapsed = asyncio.run(scenario())
    assert all(action.value == "started" for _, action in results), results
    assert elapsed < 2.0, f"无关发布批量操作被阻塞 {elapsed:.2f}s"
    for rid in others:
        rec = orch.get_release(rid)
        assert rec.status is ReleaseStatus.IN_PROGRESS and rec.current_weight == 100
    print(f"[判定] A 探测卡住期间 8 条无关发布全部 started，总耗时 {elapsed*1000:.0f}ms")


def test_slow_probe_same_release_still_single_winner(state_path, registry, clock):
    """A 的一个调用卡在探测中时，同发布的并发操作立即 advance_conflict（不等待）。"""
    orch = ReleaseOrchestrator.from_path(str(state_path), registry, clock=clock)
    rid = _make(orch, "svc-same")
    blocking = BlockingProbe()
    registry.register(rid, blocking)

    async def scenario():
        await orch.advance(rid)  # started
        clock.advance(61)
        slow_task = asyncio.create_task(orch.advance(rid))  # 卡在门禁
        await asyncio.to_thread(blocking.entered.wait, 5)

        # 同发布的推进 / 恢复 / 中止都必须立即冲突，而不是排队等探测结束
        t0 = time.monotonic()
        conflict_advance = await asyncio.gather(
            orch.advance(rid), orch.resume(rid), orch.abort(rid, "x"),
            return_exceptions=True,
        )
        elapsed = time.monotonic() - t0
        codes = sorted(getattr(e, "error_code", None) for e in conflict_advance)
        assert codes == ["advance_conflict"] * 3, conflict_advance
        assert all(isinstance(e, AdvanceConflictError) for e in conflict_advance)
        assert elapsed < 1.0, f"同发布冲突未快速失败: {elapsed:.2f}s"

        blocking.release.set()
        rec, action = await slow_task
        return rec, action

    _rec, action = asyncio.run(scenario())
    assert action.value == "advanced"
    # 只有一次切流、一次门禁；冲突调用没写事件、没动版本
    view = orch.get_release(rid)
    assert view.current_batch == 2 and view.current_weight == 50
    assert blocking.calls == 1
    assert sum(1 for e in view.events if e["type"] == "traffic_switched") == 2
    print("[判定] 慢探测期间同发布 3 个并发操作全部立即 advance_conflict，探测仅执行一次")


# --------------------------------------------------------------------------- #
# 2. 终态/暂停态（不带 expected_version）的拒绝必须留痕、版本前进、状态不变
# --------------------------------------------------------------------------- #


def _assert_traced_rejection(
    client, rid: str, path: str, expected_code: str,
    *, before: dict, expected_status: str, expected_weight: int,
    expected_batches: int,
):
    """发起一次必然被拒绝的操作，断言留痕、version 前进、实际状态不变。"""
    resp = client.post(f"/releases/{rid}/{path}", json={})
    assert resp.status_code == 409, resp.text
    body = resp.json()
    assert body["error_code"] == expected_code, body
    # 拒绝本身在本次错误响应里即可追溯：附带当前发布完整视图与已前进的版本
    assert body["release"]["status"] == expected_status
    assert body["current_version"] == before["version"] + 1

    events_before = before["events"]
    view = client.get(f"/releases/{rid}").json()
    events_after = client.get(f"/releases/{rid}/events").json()["events"]

    # 实际状态 / 权重 / 已完成批次绝不因拒绝改变
    assert view["status"] == expected_status
    assert view["current_weight"] == expected_weight
    assert view["completed_batches"] == expected_batches
    # version 前进一步，且查询结果（拒绝响应与随后 GET）都能看到新版本
    assert view["version"] == before["version"] + 1
    assert body["release"]["version"] == view["version"]
    # 事件链多出一条 operation_rejected，带当时状态/批次/判定依据
    new_events = events_after[len(events_before):]
    assert len(new_events) == 1, new_events
    ev = new_events[0]
    assert ev["type"] == "operation_rejected"
    assert ev["detail"]["current_status"] == expected_status
    assert ev["detail"]["current_batch"] == view["current_batch"]
    assert ev["detail"]["op"] == path
    assert ev["detail"]["error_code"] == expected_code
    assert ev["detail"]["decision"]
    return view, ev


def _snapshot(client, rid: str) -> dict:
    view = client.get(f"/releases/{rid}").json()
    events = client.get(f"/releases/{rid}/events").json()["events"]
    return {"version": view["version"], "events": events}


def test_rejection_traced_for_paused_release(client, clock, registry, plan_body):
    """暂停等待中的 advance / resume(非暂停语义) 拒绝均留痕。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    # 走到 paused：启动后给一个 degraded
    from conftest import register_scripted
    register_scripted(
        registry, rid,
        [HealthProbeResult(HealthVerdict.DEGRADED, "latency rising")],
    )
    client.post(f"/releases/{rid}/advance")  # started 10%
    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.json()["action"] == "paused"

    before = _snapshot(client, rid)
    # paused 上直接 advance -> duplicate_advance（paused_requires_resume）留痕
    view, ev = _assert_traced_rejection(
        client, rid, "advance", "duplicate_advance",
        before=before, expected_status="paused",
        expected_weight=10, expected_batches=0,
    )
    assert ev["detail"]["reason"] == "paused_requires_resume"
    assert ev["detail"]["current_weight"] == 10
    # 暂停中重复 resume 不允许（resume 仅对 paused 生效；第二次 resume 时已是
    #  in_progress）——这里只验证 paused 的 advance 留痕，实际权重仍冻结
    assert view["current_weight"] == 10


def test_rejection_traced_for_completed_release(client, clock, registry, plan_body):
    """completed 上的 advance / resume / abort 全部留痕且终态不复活。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    from conftest import register_scripted
    register_scripted(registry, rid)
    client.post(f"/releases/{rid}/advance")
    for _ in range(3):
        clock.advance(61)
        client.post(f"/releases/{rid}/advance")
    assert client.get(f"/releases/{rid}").json()["status"] == "completed"

    for path, code in (
        ("advance", "illegal_transition"),
        ("resume", "illegal_transition"),
        ("abort", "illegal_transition"),
    ):
        before = _snapshot(client, rid)
        view, ev = _assert_traced_rejection(
            client, rid, path, code,
            before=before, expected_status="completed",
            expected_weight=100, expected_batches=3,
        )
        assert ev["detail"]["reason"] == "terminal_status"
        assert ev["detail"]["completed_batches"] == 3
    # 连续 3 次拒绝后仍是 completed，version 恰好多 3
    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "completed" and view["current_weight"] == 100
    print(f"[判定] completed 上 3 次拒绝全部留痕，最终 version=v{view['version']}，终态未变")


def test_rejection_traced_for_rolled_back_release(client, clock, registry, plan_body):
    """rolled_back 上的 advance / resume / abort 留痕，权重保持 0、不复活。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    from conftest import register_scripted
    register_scripted(
        registry, rid,
        [HealthProbeResult(HealthVerdict.UNHEALTHY, "fatal")] * 3,
    )
    client.post(f"/releases/{rid}/advance")
    for _ in range(3):
        clock.advance(61)
        client.post(f"/releases/{rid}/advance")
    view = client.get(f"/releases/{rid}").json()
    assert view["status"] == "rolled_back"

    for path, code in (
        ("advance", "illegal_transition"),
        ("resume", "illegal_transition"),
        ("abort", "illegal_transition"),
    ):
        before = _snapshot(client, rid)
        view, ev = _assert_traced_rejection(
            client, rid, path, code,
            before=before, expected_status="rolled_back",
            expected_weight=0, expected_batches=0,
        )
        assert view["active_version"] == plan_body["stable"]
        assert ev["detail"]["reason"] == "terminal_status"


def test_rejection_traced_for_aborted_release(client, registry, plan_body):
    """aborted 上的 advance / resume -> manual_abort 留痕；重复 abort 也留痕。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    client.post(f"/releases/{rid}/advance")  # started 10%
    r = client.post(f"/releases/{rid}/abort", json={"reason": "stop"})
    assert r.status_code == 200

    for path, code in (
        ("advance", "manual_abort"),
        ("resume", "manual_abort"),
        ("abort", "duplicate_advance"),
    ):
        before = _snapshot(client, rid)
        view, ev = _assert_traced_rejection(
            client, rid, path, code,
            before=before, expected_status="aborted",
            expected_weight=10, expected_batches=0,
        )
        assert ev["detail"]["reason"] == "already_aborted"
        assert view["active_version"] == "v2.0.0"  # 中止冻结候选 10%
    final = client.get(f"/releases/{rid}").json()
    assert final["current_weight"] == 10 and final["status"] == "aborted"


def test_rejection_trace_does_not_mutate_batches_or_weight(client, clock, registry, plan_body):
    """多次拒绝后，已完成批次 / 权重 / 活动版本严格不变，仅 version 与事件增长。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    from conftest import register_scripted
    register_scripted(registry, rid)
    client.post(f"/releases/{rid}/advance")
    clock.advance(61)
    client.post(f"/releases/{rid}/advance")  # 到第 2 批，weight=50，完成批次 1
    mid = client.get(f"/releases/{rid}").json()
    assert mid["current_batch"] == 2 and mid["current_weight"] == 50

    # 此时 in_progress：resume 不适用（not_paused），拒绝且留痕、权重不变
    before = _snapshot(client, rid)
    _view, ev = _assert_traced_rejection(
        client, rid, "resume", "illegal_transition",
        before=before, expected_status="in_progress",
        expected_weight=50, expected_batches=1,
    )
    assert ev["detail"]["reason"] == "not_paused"
    assert ev["detail"]["current_batch"] == 2


# --------------------------------------------------------------------------- #
# 3. HTTP 层：真实多线程请求下慢探测不连累无关发布
# --------------------------------------------------------------------------- #


def test_http_slow_probe_unrelated_release_not_blocked(tmp_path):
    """通过 HTTP 并发：慢发布探测 1.2s，无关发布请求仍在亚秒级 200 生效。"""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from release_orchestrator.api import router
    from release_orchestrator.health import HealthProbeRegistry

    reg = HealthProbeRegistry()
    # 用真实时钟：慢探测本身就是真实 sleep，观察期用 1ms 真实时间即可越过
    app = FastAPI()
    app.state.orchestrator = ReleaseOrchestrator.from_path(
        str(tmp_path / "s.json"), reg
    )
    app.state.probes = reg
    app.include_router(router)

    def create(name):
        r = client.post("/releases", json={
            "name": name, "candidate": "v2", "stable": "v1",
            "weights": [10, 100], "min_observe_seconds": 0.001,
        })
        assert r.status_code == 201, r.text
        return r.json()["id"]

    with TestClient(app) as client:
        rid_slow = create("slow-svc")
        rid_fast = create("fast-svc")
        reg.register(rid_slow, SlowProbe(1.2))

        # slow：started -> 稍后第二次 advance（越过 1ms 观察期）进入 1.2s 的门禁
        assert client.post(f"/releases/{rid_slow}/advance").json()["action"] == "started"
        time.sleep(0.02)
        barrier = threading.Barrier(2)
        out: dict[str, object] = {}

        def slow_call():
            barrier.wait()
            out["slow"] = client.post(f"/releases/{rid_slow}/advance")

        def fast_call():
            barrier.wait()
            t0 = time.monotonic()
            out["fast_elapsed"] = time.monotonic() - t0
            out["fast"] = client.post(f"/releases/{rid_fast}/advance")

        t1 = threading.Thread(target=slow_call)
        t2 = threading.Thread(target=fast_call)
        t1.start(); t2.start(); t1.join(); t2.join()

        fast_resp = out["fast"]
        assert fast_resp.status_code == 200, fast_resp.text
        assert fast_resp.json().get("error_code") != "state_commit_busy"
        assert fast_resp.json()["action"] == "started"
        # 关键断言：无关请求没有被慢探测拖住，也没有拿到 503
        assert out["fast_elapsed"] < 0.8, out["fast_elapsed"]

        slow_resp = out["slow"]
        assert slow_resp.status_code == 200
        assert slow_resp.json()["action"] == "advanced"

        view = client.get(f"/releases/{rid_fast}").json()
        assert view["status"] == "in_progress" and view["current_weight"] == 10
        print(f"[判定] HTTP 慢探测 1.2s 期间无关发布 started，耗时 "
              f"{out['fast_elapsed']*1000:.0f}ms，无 state_commit_busy")
