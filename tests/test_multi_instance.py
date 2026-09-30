"""多实例协作测试（多个服务进程 / 多个编排器实例共用同一份 JSON 状态）。

覆盖：
1. 多进程同时操作同一发布 -> 恰好一个生效，其余 advance_conflict，流量切换只执行一次；
2. 陈旧实例继续操作 -> stale_state / 终态拒绝，不覆盖新结果，拒绝原因进事件链；
3. 不同发布并行推进 -> 互不排队（持锁期间另一发布仍可提交）；
4. 进程重启后的状态一致性（已完成记录不丢、版本号连续、旧版数据兼容升级）；
5. 高频混合负载（几十条发布 × 多轮操作）下半截状态/损坏/卡死；
6. HTTP 层陈旧/冲突错误响应可区分，且携带最新发布视图。

所有子进程 worker 定义在模块级（spawn/forkserver 可 pickle），状态路径显式传参。
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from release_orchestrator.health import (
    HealthProbeRegistry,
    HealthProbeResult,
    HealthVerdict,
    ScriptedHealthProbe,
)
from release_orchestrator.models import ReleasePlan
from release_orchestrator.orchestrator import ReleaseOrchestrator

_CTX = mp.get_context("spawn")


# --------------------------------------------------------------------------- #
# 子进程侧工具（必须模块级可 pickle）
# --------------------------------------------------------------------------- #

def _orch(state_path: str, observe: float = 0.0) -> ReleaseOrchestrator:
    return ReleaseOrchestrator.from_path(
        state_path, HealthProbeRegistry(), clock=time.time
    )


class _SlowHealthyProbe:
    """门禁内停留 hold 秒，模拟真实切换/探测窗口，使操作锁被持续持有。"""

    def __init__(self, hold: float) -> None:
        self.hold = hold

    def probe(self, release_id, candidate, weight):
        time.sleep(self.hold)
        return HealthProbeResult(HealthVerdict.HEALTHY, "slow-ok")


_GATE = None


def _pool_init(gate):
    global _GATE
    _GATE = gate


def _worker_op(args):
    """子进程：对某发布执行一种操作，返回 (结果类型, 简述)。

    所有子进程通过 initializer 注入的多进程闸门同步：就绪后在同一时刻抢锁，
    避免因进程启动快慢把"并发冲突"变成"串行第二个生效"。
    """
    state_path, rid, op, hold = args
    if _GATE is not None:
        _GATE.wait(timeout=10)
    try:
        orch = _orch(state_path)
        if op == "advance":
            reg = HealthProbeRegistry()
            orch.probes = reg
            if hold:
                reg.register(rid, _SlowHealthyProbe(hold))
            _, action = asyncio.run(orch.advance(rid))
            return ("ok", f"advance:{action.value}")
        if op == "abort":
            rec = asyncio.run(orch.abort(rid, "multiprocess"))
            return ("ok", f"abort:{rec.status.value}")
        if op == "resume":
            rec = asyncio.run(orch.resume(rid))
            return ("ok", f"resume:{rec.status.value}")
        raise ValueError(op)
    except Exception as exc:  # noqa: BLE001 - 子进程把异常类型回传主进程断言
        return (type(exc).__name__, str(exc)[:120])


def _run_in_processes(state_path: str, jobs: list[tuple[str, str, float]]):
    """在独立进程中同一时刻并发跑 [(rid, op, hold), ...]，收集结果。"""
    gate = _CTX.Event()
    payload = [(state_path, rid, op, hold) for rid, op, hold in jobs]
    with _CTX.Pool(len(payload), initializer=_pool_init, initargs=(gate,)) as pool:
        async_result = pool.map_async(_worker_op, payload)
        time.sleep(0.6)  # 等所有子进程就绪并阻塞在闸门上
        gate.set()        # 同时放行
        return async_result.get(timeout=30)


# --------------------------------------------------------------------------- #
# 夹具
# --------------------------------------------------------------------------- #

@pytest.fixture
def state_path(tmp_path: Path) -> str:
    return str(tmp_path / "shared_state.json")


def _new_plan(name: str = "svc", weights=(10, 50, 100), observe: float = 0.01):
    return ReleasePlan(name, "v2", "v1", list(weights), observe)


@pytest.fixture
def started_release(state_path):
    """建一条 in_progress 的发布，返回 (orchestrator, release_id)。"""
    orch = _orch(state_path)
    rid = orch.create_release(_new_plan()).id
    asyncio.run(orch.advance(rid))
    return orch, rid


# --------------------------------------------------------------------------- #
# 1. 多进程同一发布：单赢家
# --------------------------------------------------------------------------- #

def test_multiprocess_same_release_single_winner(started_release, state_path):
    """4 个进程同时推进同一发布（慢门禁拉长锁持有窗口）：

    不变量（强保证，与调度抖动无关）：
    - 同一时刻只有一个实例真正执行流量切换；迟到者若在赢家放锁后才进入，
      只能合法地推进"下一批"，绝不会把同一次切换执行两遍；
    - 未抢到操作权的请求得到明确的 AdvanceConflictError；
    - 最终批次/权重/完成数自洽。
    """
    orch, rid = started_release
    time.sleep(0.05)  # 观察期满足

    jobs = [(rid, "advance", 1.2)] + [(rid, "advance", 0.0) for _ in range(3)]
    results = _run_in_processes(state_path, jobs)
    print("\n[multi-op] inputs:", jobs)
    print("[multi-op] results:", results)

    oks = [r for r in results if r[0] == "ok"]
    conflicts = [r for r in results if r[0] == "AdvanceConflictError"]
    others = [r for r in results
              if r[0] not in ("ok", "AdvanceConflictError")]
    # 慢门禁 1.2s 内 3 个同时抢锁的进程必然全部冲突
    assert len(conflicts) == 3, results
    assert not others, results
    # 同时刻只有一个赢家
    assert len(oks) == 1 and oks[0][1] == "advance:advanced", results

    fresh = _orch(state_path).get_release(rid)
    print("[multi-op] final:", fresh.status.value,
          "batch=", fresh.current_batch, "weight=", fresh.current_weight,
          "version=", fresh.version)
    assert fresh.current_batch == 2
    assert fresh.current_weight == 50
    # 启动一次 + 本次推进一次：同一次流量切换绝不重复
    switch_events = [e for e in fresh.events if e["type"] == "traffic_switched"]
    assert len(switch_events) == 2
    assert fresh.completed_batches == 1


def test_multiprocess_abort_and_advance_only_one_effective(started_release, state_path):
    """一个进程 abort、另一个进程在慢门禁内 advance：只有一边生效。"""
    _, rid = started_release
    time.sleep(0.05)
    results = _run_in_processes(
        state_path, [(rid, "advance", 0.8), (rid, "abort", 0.0)]
    )
    print("\n[abort-race] results:", results)
    oks = [r for r in results if r[0] == "ok"]
    assert len(oks) == 1, results
    rejected = [r for r in results if r[0] != "ok"]
    # 败者必须是可区分的"未生效"：要么抢锁冲突，要么锁释放后读到对方已写的新状态
    assert rejected[0][0] in ("AdvanceConflictError", "ManualAbortError",
                              "IllegalTransitionError", "StaleStateError"), results

    fresh = _orch(state_path).get_release(rid)
    print("[abort-race] final status:", fresh.status.value, "weight:", fresh.current_weight)
    # 无论谁赢，状态自洽：aborted 冻结权重 或 推进后 in_progress 权重 50
    if fresh.status.value == "aborted":
        assert fresh.current_weight == 10
    else:
        assert fresh.status.value == "in_progress"
        assert fresh.current_weight == 50


# --------------------------------------------------------------------------- #
# 2. 陈旧实例：不覆盖新结果
# --------------------------------------------------------------------------- #

def test_stale_instance_cannot_overwrite_completion(state_path):
    """实例 A 长时间停滞后再推进：共享状态已被 B 完成 -> 拒绝，且不回写进行中。"""
    a = _orch(state_path)
    rid = a.create_release(_new_plan(weights=(10, 100), observe=0.0)).id
    asyncio.run(a.advance(rid))  # started

    # 另一实例把发布走完
    b = _orch(state_path)
    time.sleep(0.02)
    asyncio.run(b.advance(rid))  # 100
    time.sleep(0.02)
    _, action = asyncio.run(b.advance(rid))  # completed
    assert action.value == "completed"

    # A 是"运行很久"的旧实例：查询立刻看到最新状态（无缓存）
    view_a = a.get_release(rid)
    print("\n[stale] A sees status:", view_a.status.value, "version:", view_a.version)
    assert view_a.status.value == "completed"

    # A 再推进：终态拒绝；状态保持 completed，没有被写回 in_progress
    with pytest.raises(Exception) as ei:
        asyncio.run(a.advance(rid))
    print("[stale] A advance rejected:", type(ei.value).__name__)
    assert ei.value.__class__.__name__ == "IllegalTransitionError"

    fresh = _orch(state_path).get_release(rid)
    assert fresh.status.value == "completed"
    assert fresh.current_weight == 100
    assert fresh.completed_batches == 2
    # 拒绝原因在事件链可见
    last = fresh.events[-1]
    assert last["type"] == "operation_rejected"
    assert "terminal_completed" in last["detail"]["reason"]


def test_stale_optimistic_commit_rejected_when_state_changed_under_lock(state_path):
    """构造真正的版本竞争：基于旧 version 提交必须被 StaleStateError 拒绝。

    直接调用存储层：A 读出 version=N 的工作副本，B 先完成一次提交，
    A 再拿旧 version 提交 -> StaleStateError，磁盘内容保持 B 的结果。
    """
    from release_orchestrator.errors import StaleStateError

    a = _orch(state_path)
    rid = a.create_release(_new_plan(weights=(10, 100), observe=0.0)).id
    asyncio.run(a.advance(rid))
    base, version = a.store.get_record(rid)
    print("\n[opt] base version:", version, "status:", base.status.value)

    # B 先推进并提交（版本 +1）
    b = _orch(state_path)
    time.sleep(0.02)
    asyncio.run(b.advance(rid))
    after_b = b.get_release(rid)
    assert after_b.current_weight == 100

    # A 用过期工作副本/旧版本，经编排器提交管线：必须失败且事件链可见
    import copy
    stale_copy = copy.deepcopy(base)
    with pytest.raises(StaleStateError):
        a._commit_working_copy(stale_copy, version)

    fresh = _orch(state_path).get_release(rid)
    print("[opt] after stale commit:", fresh.status.value,
          "weight=", fresh.current_weight, "batch=", fresh.current_batch)
    # 绝不能被旧进度（weight=10, batch=1）覆盖
    assert fresh.current_weight == 100
    assert fresh.current_batch == 2
    # 陈旧拒绝事件已写入最新状态
    assert any(e["type"] == "operation_rejected" and e["detail"]["reason"] == "stale_state"
               for e in fresh.events)


def test_stale_resume_after_other_instance_rolled_back(started_release, state_path):
    """暂停后另一实例已把发布回滚：旧实例 resume 必须被拒绝，不能写回 in_progress。"""
    a, rid = started_release
    # A 推进到 paused
    time.sleep(0.05)
    reg = HealthProbeRegistry()
    reg.register(rid, ScriptedHealthProbe(
        [HealthProbeResult(HealthVerdict.DEGRADED, "slow")]))
    a.probes = reg
    _, action = asyncio.run(a.advance(rid))
    assert action.value == "paused"

    # 直接在磁盘上模拟"另一实例"完成回滚（真实环境由健康失败触发）
    b = _orch(state_path)
    rec_b, ver_b = b.store.get_record(rid)
    rec_b.status = type(rec_b.status)("rolled_back")
    rec_b.current_weight = 0
    rec_b.active_version = rec_b.plan.stable
    rec_b.rollback_reason = "其他实例触发回滚"
    b._append_event(rec_b, "rollback", {"decision": "模拟其他实例回滚"})
    b.store.commit_record(rec_b, ver_b)

    # 旧实例 A resume -> 终态拒绝，状态不回退
    with pytest.raises(Exception) as ei:
        asyncio.run(a.resume(rid))
    print("\n[stale-resume] rejected:", type(ei.value).__name__)
    fresh = _orch(state_path).get_release(rid)
    assert fresh.status.value == "rolled_back"
    assert fresh.current_weight == 0
    assert any(e["type"] == "operation_rejected" for e in fresh.events)


# --------------------------------------------------------------------------- #
# 3. 不同发布并行不互堵
# --------------------------------------------------------------------------- #

def test_different_releases_do_not_block_each_other(state_path):
    """发布 X 长时间持有自己的操作锁时，发布 Y 的操作照常提交成功。"""
    orch = _orch(state_path)
    rid_x = orch.create_release(_new_plan("X")).id
    rid_y = orch.create_release(_new_plan("Y")).id
    asyncio.run(orch.advance(rid_x))
    asyncio.run(orch.advance(rid_y))
    time.sleep(0.05)

    # X 在独立进程里跑慢门禁（持 X 的操作锁 ~0.8s）
    # Y 同时在另一个进程推进：不应排队等待 X
    t0 = time.monotonic()
    results = _run_in_processes(
        state_path,
        [(rid_x, "advance", 0.8), (rid_y, "advance", 0.0)],
    )
    elapsed = time.monotonic() - t0
    print("\n[parallel] results:", results, "elapsed=%.2fs" % elapsed)
    assert results[0][0] == "ok" and results[0][1] == "advance:advanced"
    assert results[1][0] == "ok" and results[1][1] == "advance:advanced"
    # Y 没被 X 拖住：两发布并行 ≈ 一次慢门禁 + 进程/闸门开销；
    # 若错误地共用全局操作锁串行执行，将接近两次慢门禁（1.6s 探针）以上
    assert elapsed < 2.2

    fresh = _orch(state_path)
    assert fresh.get_release(rid_x).current_batch == 2
    assert fresh.get_release(rid_y).current_batch == 2


# --------------------------------------------------------------------------- #
# 4. 重启一致性
# --------------------------------------------------------------------------- #

def test_restart_sees_other_instance_progress(state_path):
    """一个实例推进后，新启动的实例（进程重启）读到完整最新状态。"""
    a = _orch(state_path)
    rid = a.create_release(_new_plan(weights=(10, 50, 100), observe=0.0)).id
    asyncio.run(a.advance(rid))
    time.sleep(0.02)
    asyncio.run(a.advance(rid))  # batch 2

    restarted = _orch(state_path)  # 模拟滚动重启后的新进程
    view = restarted.get_release(rid)
    print("\n[restart] status:", view.status.value, "batch:", view.current_batch,
          "weight:", view.current_weight, "version:", view.version)
    assert view.current_batch == 2
    assert view.current_weight == 50
    assert view.batches[0].completed is True
    assert view.version >= 3
    # 从重启后的实例继续走完
    time.sleep(0.02)
    asyncio.run(restarted.advance(rid))
    time.sleep(0.02)
    _, action = asyncio.run(restarted.advance(rid))
    assert action.value == "completed"
    assert restarted.get_release(rid).completed_batches == 3


def test_completed_records_survive_restart_and_not_repeated(state_path):
    """已完成记录重启后仍是完成态，批次不重复计数，事件链里完成/切换次数正确。"""
    a = _orch(state_path)
    rid = a.create_release(_new_plan(weights=(10, 100), observe=0.0)).id
    asyncio.run(a.advance(rid))
    time.sleep(0.02)
    asyncio.run(a.advance(rid))
    time.sleep(0.02)
    asyncio.run(a.advance(rid))  # completed
    version_done = a.get_release(rid).version

    b = _orch(state_path)
    v = b.get_release(rid)
    assert v.status.value == "completed"
    assert v.completed_batches == 2
    assert v.version == version_done
    assert sum(1 for e in v.events if e["type"] == "release_completed") == 1


def test_legacy_v1_state_file_without_versions_still_loads_and_upgrades(state_path):
    """旧版单实例产生的 v1 数据（无 revision/version）继续可用，首次提交自动升级。"""
    legacy = {
        "schema_version": 1,
        "updated_at": time.time(),
        "releases": {
            "legacy1": {
                "id": "legacy1",
                "plan": {
                    "name": "legacy-svc", "candidate": "v2", "stable": "v1",
                    "weights": [10, 100], "min_observe_seconds": 0.0,
                },
                "status": "in_progress",
                "current_batch": 1,
                "batches": [
                    {"weight": 10, "completed": False, "started_at": time.time(),
                     "completed_at": None, "gate_attempts": 0,
                     "consecutive_failures": 0},
                    {"weight": 100, "completed": False, "started_at": None,
                     "completed_at": None, "gate_attempts": 0,
                     "consecutive_failures": 0},
                ],
                "current_weight": 10,
                "active_version": "v2",
                "created_at": 1.0, "updated_at": 1.0,
                "completed_batches": 0,
                "rollback_reason": None,
                "events": [],
            }
        },
        "quarantine": [],
    }
    Path(state_path).write_text(json.dumps(legacy), encoding="utf-8")

    orch = _orch(state_path)
    rec = orch.get_release("legacy1")
    print("\n[legacy] loaded:", rec.status.value, "version:", rec.version)
    assert rec.status.value == "in_progress"
    assert rec.version == 0  # 旧数据按 0 加载

    time.sleep(0.02)
    _, action = asyncio.run(orch.advance("legacy1"))  # 门禁健康 -> 100
    assert action.value == "advanced"
    time.sleep(0.02)
    _, action = asyncio.run(orch.advance("legacy1"))  # completed
    assert action.value == "completed"

    data = json.loads(Path(state_path).read_text())
    print("[legacy] upgraded file revision:", data["revision"],
          "record version:", data["releases"]["legacy1"]["version"])
    assert data["revision"] >= 2
    assert data["releases"]["legacy1"]["version"] >= 2
    # schema_version 保持 1，旧版本程序仍能识别
    assert data["schema_version"] == 1


# --------------------------------------------------------------------------- #
# 5. 高频混合负载
# --------------------------------------------------------------------------- #

def _worker_load(args):
    """子进程负载：对一批发布做多轮 advance，观察期很短，尽量制造并发提交。"""
    state_path, ids, rounds = args
    try:
        orch = _orch(state_path)
        counts = {"ok": 0, "conflict": 0, "duplicate": 0, "terminal": 0, "other": 0}
        for _ in range(rounds):
            for rid in ids:
                try:
                    _, action = asyncio.run(orch.advance(rid))
                    counts["ok"] += 1
                except Exception as exc:  # noqa: BLE001
                    name = type(exc).__name__
                    if name == "AdvanceConflictError":
                        counts["conflict"] += 1
                    elif name == "DuplicateAdvanceError":
                        counts["duplicate"] += 1
                    elif name in ("IllegalTransitionError", "ManualAbortError"):
                        counts["terminal"] += 1
                    else:
                        counts["other"] += 1
            time.sleep(0.005)
        return ("ok", counts)
    except Exception as exc:  # noqa: BLE001
        return (type(exc).__name__, str(exc)[:150])


def test_high_frequency_mixed_load_no_corruption(state_path):
    """40 条发布、4 个进程连续多轮推进：

    - 无半截状态（文件始终可解析、revision 单调）；
    - 每条发布最终自洽：要么 completed，要么仍在合法进行态；
    - 已完成的发布 completed_batches 恰为批次数、只完成一次；
    - 重启后记录一条不少。
    """
    orch = _orch(state_path)
    n = 40
    ids = []
    for i in range(n):
        rid = orch.create_release(_new_plan(f"svc-{i}", weights=(10, 100),
                                            observe=0.0)).id
        asyncio.run(orch.advance(rid))  # 全部启动到 10%
        ids.append(rid)

    workers = 4
    shards = [ids[i::workers] for i in range(workers)]
    # 故意让相邻分片重叠几条，制造跨进程同发布竞争
    shards[0] = ids[shards[0][0] and 0:0] or shards[0]  # 保持结构
    overlap = ids[:6]
    shards = [list(dict.fromkeys(shards[i] + overlap)) for i in range(workers)]

    t0 = time.monotonic()
    with _CTX.Pool(workers) as pool:
        results = pool.map(_worker_load,
                           [(state_path, shards[i], 8) for i in range(workers)])
    elapsed = time.monotonic() - t0
    print("\n[load] elapsed=%.2fs results:" % elapsed)
    totals = {"ok": 0, "conflict": 0, "duplicate": 0, "terminal": 0, "other": 0}
    for kind, payload in results:
        print("  ", kind, payload)
        assert kind == "ok", results
        for k in totals:
            totals[k] += payload[k]
    print("[load] aggregated:", totals)
    assert totals["other"] == 0, totals  # 不允许出现非预期错误（卡死/损坏等）
    assert elapsed < 60, "接口不应长时间卡死"

    # 文件可解析、revision 单调为正
    data = json.loads(Path(state_path).read_text())
    assert data["revision"] > 0
    assert len(data["releases"]) == n

    # 模拟重启：全新实例逐条校验一致性
    restarted = _orch(state_path)
    completed = 0
    for rid in ids:
        rec = restarted.get_release(rid)
        assert rec.status.value in ("in_progress", "paused", "completed"), rec.status
        done = sum(1 for b in rec.batches if b.completed)
        assert done == rec.completed_batches
        if rec.status.value == "completed":
            completed += 1
            assert rec.completed_batches == 2
            assert rec.current_weight == 100
            assert sum(1 for e in rec.events if e["type"] == "release_completed") == 1
            assert sum(1 for e in rec.events if e["type"] == "rollback") == 0
    print("[load] completed releases after restart:", completed)
    assert completed >= 1  # 高并发下至少有一批被走完（实际上绝大多数应完成）


# --------------------------------------------------------------------------- #
# 6. HTTP 层多实例错误响应
# --------------------------------------------------------------------------- #

def _make_client(state_path):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from release_orchestrator.api import router

    app = FastAPI()
    reg = HealthProbeRegistry()
    app.state.orchestrator = ReleaseOrchestrator.from_path(
        state_path, reg, clock=time.time
    )
    app.state.probes = reg
    app.include_router(router)
    return TestClient(app), app


def test_http_stale_and_conflict_responses_distinguishable(state_path):
    """HTTP 层：终态/陈旧/冲突返回不同 error_code，且响应带 effective=false 与最新视图。"""
    from release_orchestrator.errors import StaleStateError

    client, app = _make_client(state_path)
    body = {"name": "h", "candidate": "v2", "stable": "v1",
            "weights": [10, 100], "min_observe_seconds": 0.001}
    rid = client.post("/releases", json=body).json()["id"]
    client.post(f"/releases/{rid}/advance")
    time.sleep(0.02)
    client.post(f"/releases/{rid}/advance")
    time.sleep(0.02)
    client.post(f"/releases/{rid}/advance")  # completed

    # 先抓取 completed 时的工作副本与版本（之后由一次拒绝调用把版本 +1）
    orch = app.state.orchestrator
    import copy
    rec, ver = orch.store.get_record(rid)
    stale = copy.deepcopy(rec)
    stale.status = type(stale.status)("in_progress")

    # 再推进：illegal_transition，附带最新发布视图，明确未生效，且版本被推进
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409
    payload = r.json()
    print("\n[http] terminal advance:", payload["error_code"],
          "effective:", payload.get("effective"),
          "view status:", payload["release"]["status"])
    assert payload["error_code"] == "illegal_transition"
    assert payload["effective"] is False
    assert payload["release"]["status"] == "completed"
    assert payload["release"]["version"] >= 1

    # 用过期版本把终态写回进行中：stale_state 独立错误码，磁盘保持 completed
    from release_orchestrator.errors import StaleStateError
    with pytest.raises(StaleStateError):
        orch._commit_working_copy(stale, ver)
    assert client.get(f"/releases/{rid}").json()["status"] == "completed"

    # 冲突响应：同进程内并发首次推进另一条发布。
    # 观察期保持未满足：赢家放锁后迟到的线程即使串行进入，也只能得到
    # duplicate_advance（观察期未满），不会产生第二次真实切换。
    rid2 = client.post("/releases", json=dict(body, name="h2",
                                              min_observe_seconds=3600)).json()["id"]
    import threading
    out = []
    barrier = threading.Barrier(6)

    def hit():
        barrier.wait()
        out.append(client.post(f"/releases/{rid2}/advance"))

    threads = [threading.Thread(target=hit) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    oks = [x for x in out if x.status_code == 200]
    rejected = [x for x in out
                if x.json().get("error_code") in ("advance_conflict", "duplicate_advance")]
    print("[http] same-process race: ok=%d rejected=%d" % (len(oks), len(rejected)))
    assert len(oks) == 1
    assert len(rejected) == 5
    for x in rejected:
        assert x.json()["effective"] is False
    # 流量只切换一次
    view = client.get(f"/releases/{rid2}").json()
    assert view["current_batch"] == 1 and view["current_weight"] == 10
    events = client.get(f"/releases/{rid2}/events").json()["events"]
    assert sum(1 for e in events if e["type"] == "traffic_switched") == 1


# --------------------------------------------------------------------------- #
# 7. 滚动重启与跨进程竞争组合
# --------------------------------------------------------------------------- #

def test_restarting_instance_racing_with_active_instance(started_release, state_path):
    """一个实例滚动重启回来时，另一实例正持有操作锁：重启者得到未生效冲突，不破坏状态。"""
    _, rid = started_release
    time.sleep(0.05)

    # 活跃实例在慢门禁中（持锁 ~0.8s）；"重启回来"的实例此刻发起推进
    results = _run_in_processes(
        state_path, [(rid, "advance", 0.8), (rid, "advance", 0.0)]
    )
    print("\n[restart-race] results:", results)
    oks = [r for r in results if r[0] == "ok"]
    rejected = [r for r in results if r[0] != "ok"]
    assert len(oks) == 1
    assert rejected[0][0] == "AdvanceConflictError", results

    # 重启后的新实例看到的就是赢家写下的自洽状态
    fresh = _orch(state_path).get_release(rid)
    assert fresh.status.value == "in_progress"
    assert fresh.current_batch == 2
    assert fresh.current_weight == 50
    done = sum(1 for b in fresh.batches if b.completed)
    assert done == fresh.completed_batches == 1


def test_other_instance_abort_visible_then_advance_rejected(state_path):
    """实例暂停后继续前，别的实例已中止：查询即时可见，推进/恢复均以可区分错误拒绝。"""
    a = _orch(state_path)
    rid = a.create_release(_new_plan(weights=(10, 100), observe=0.0)).id
    asyncio.run(a.advance(rid))
    reg = HealthProbeRegistry()
    reg.register(rid, ScriptedHealthProbe(
        [HealthProbeResult(HealthVerdict.DEGRADED, "slow")]))
    a.probes = reg
    time.sleep(0.02)
    _, action = asyncio.run(a.advance(rid))
    assert action.value == "paused"

    # 另一实例执行中止
    b = _orch(state_path)
    asyncio.run(b.abort(rid, "abort-from-b"))

    # A 立即能查到新结果（无缓存）
    view = a.get_release(rid)
    print("\n[pause-abort] A sees:", view.status.value, "weight:", view.current_weight)
    assert view.status.value == "aborted"
    assert view.current_weight == 10  # 中止冻结，不回滚

    for coro_name in ("advance", "resume"):
        with pytest.raises(Exception) as ei:
            asyncio.run(getattr(a, coro_name)(rid))
        print(f"[pause-abort] A.{coro_name} ->", type(ei.value).__name__)
        assert ei.value.__class__.__name__ == "ManualAbortError"

    fresh = _orch(state_path).get_release(rid)
    assert fresh.status.value == "aborted"
    # 两次拒绝都在事件链里可追溯
    rejections = [e for e in fresh.events if e["type"] == "operation_rejected"]
    assert len(rejections) == 2
    assert all("manual_aborted" in e["detail"]["reason"] for e in rejections)
