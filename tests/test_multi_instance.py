"""多实例协作测试：多个本地服务进程共享同一份发布状态。

覆盖：
- 多进程同时推进/恢复/中止同一发布：恰好一个生效，其余以可区分方式返回未生效，
  流量切换不重复、事件不丢、版本单调；
- 陈旧实例（运行很久/暂停后/重启回来）携带旧 version 继续操作：stale_version 拒绝，
  事件留痕，状态绝不回退；
- 不同发布并行推进：互不排队堵住，几十条发布短时间内全部精确完成一次；
- 进程滚动重启后的状态一致性：已完成记录不丢、批次不重复、回滚只发生一次。

测试过程中 INFO 日志继续打印输入、状态变化与判定依据（见 conftest）。
"""

from __future__ import annotations

import json
import subprocess
import time

import pytest
import sys
from pathlib import Path

from release_orchestrator.health import HealthProbeRegistry, ScriptedHealthProbe
from release_orchestrator.models import (
    HealthProbeResult,
    HealthVerdict,
    ReleasePlan,
    ReleaseStatus,
)
from release_orchestrator.orchestrator import ReleaseOrchestrator

ROOT = Path(__file__).resolve().parent.parent
WORKER = ROOT / "tests" / "_multi_process_worker.py"


# --------------------------------------------------------------------------- #
# 驱动工具
# --------------------------------------------------------------------------- #

def _spawn(cfg: dict) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(WORKER), json.dumps(cfg)],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def run_instances(cfgs: list[dict]) -> list[dict]:
    """同时启动多个实例进程，等全部结束并收集各自的 JSON 结果。"""
    procs = [_spawn(cfg) for cfg in cfgs]
    results: list[dict] = []
    for p in procs:
        out, err = p.communicate(timeout=60)
        assert p.returncode == 0, f"worker failed: {err}"
        payload = json.loads(out)
        assert "fatal" not in payload, payload
        results.append(payload)
    return results


def make_release(
    state_path: Path, weights=(10, 50, 100), name="multi-svc",
    max_failures: int = 3,
) -> str:
    orch = ReleaseOrchestrator.from_path(
        str(state_path), HealthProbeRegistry(), clock=lambda: 1_000_000.0
    )
    rec = orch.create_release(
        ReleasePlan(name, "v2", "v1", list(weights), 0.0,
                    max_consecutive_failures=max_failures)
    )
    return rec.id


def cfg(instance: str, state: Path, rid: str, calls: list[dict], **kw) -> dict:
    base = {
        "instance_id": instance,
        "state": str(state),
        "release_id": rid,
        "calls": calls,
        "fake_clock": True,
    }
    base.update(kw)
    return base


def load_final(state_path: Path, rid: str) -> object:
    return ReleaseOrchestrator.from_path(
        str(state_path), HealthProbeRegistry(), clock=lambda: 1_000_000.0
    ).get_release(rid)


# --------------------------------------------------------------------------- #
# 1. 多进程同时推进同一发布
# --------------------------------------------------------------------------- #

def test_multi_process_advance_single_winner(state_path: Path):
    """4 个进程同时对同一发布连续推进：每次切换只发生一次，其余调用明确未生效。

    非阻塞抢锁下，16 次调用中能有几次抢到锁取决于进程调度（生效次数
    不少于 1 即可），因此竞速阶段按不变量判定；随后用一个"退避重试直到
    生效"的实例确定性收尾，再校验终态一致性。
    """
    rid = make_release(state_path)
    n_procs, rounds = 4, 4
    results = run_instances([
        cfg(f"inst-{i}", state_path, rid, [{"op": "advance"} for _ in range(rounds)],
            fake_clock=False)
        for i in range(n_procs)
    ])

    flat = [c for r in results for c in r["results"]]
    oks = [c for c in flat if "ok" in c]
    rejects = [c for c in flat if "err" in c]

    # 每次调用都有明确结果：要么生效，要么以可区分错误码未生效（无"半生效"）
    assert len(oks) + len(rejects) == n_procs * rounds
    assert {c["err"] for c in rejects} <= {
        "advance_conflict", "duplicate_advance", "illegal_transition"
    }
    # 生效调用按 version 排序（即真实时序）必须是规范序列的无重复前缀：
    # 同一次流量切换不会执行两遍，也不会跳批
    canonical = ["started", "advanced", "advanced", "completed"]
    oks_by_version = sorted(oks, key=lambda c: c["version"])
    effective = [c["ok"] for c in oks_by_version]
    assert effective == canonical[: len(effective)], effective
    assert [c["version"] for c in oks_by_version] == list(range(3, 3 + len(effective)))

    # 确定性收尾：模拟真实调用方对"未生效"结果退避重试，直到发布完成
    drain = run_instances([
        cfg("drain", state_path, rid,
            [{"op": "advance", "retry_until_effective": True} for _ in range(4)],
            fake_clock=False)
    ])
    drain_flat = drain[0]["results"]
    assert all("ok" in c or c["err"] == "illegal_transition" for c in drain_flat)

    final = load_final(state_path, rid)
    assert final.status is ReleaseStatus.COMPLETED
    assert final.completed_batches == 3
    assert final.current_weight == 100
    assert final.active_version == "v2"
    # 每次切流只发生一次，完成事件唯一，事件 seq 连续无重复
    assert sum(1 for e in final.events if e["type"] == "traffic_switched") == 3
    assert sum(1 for e in final.events if e["type"] == "release_completed") == 1
    assert [e["seq"] for e in final.events] == list(range(1, len(final.events) + 1))
    # create + 4 次生效推进 -> v6；此后每次撞终态的拒绝都会留
    # operation_rejected 事件并让 version 前进（但状态/权重/批次不变）；
    # 抢锁冲突（advance_conflict）不留痕、不动 version
    all_calls = flat + drain_flat
    n_terminal_rejects = sum(1 for c in all_calls if c.get("err") == "illegal_transition")
    assert final.version == 6 + n_terminal_rejects
    rej_events = [e for e in final.events if e["type"] == "operation_rejected"]
    assert len(rej_events) == n_terminal_rejects
    assert all(e["detail"]["reason"] == "terminal_status" for e in rej_events)
    assert all(e["detail"]["current_status"] == "completed" for e in rej_events)
    print(f"[判定] 竞速生效 {len(effective)} 次（{effective}），"
          f"拒绝 {len(all_calls) - 4} 次（终态拒绝留痕 {n_terminal_rejects} 次），"
          f"最终 version=v{final.version}")


def test_multi_process_abort_single_winner(state_path: Path):
    """多进程同时中止同一发布：只有一个生效，其余得到冲突/重复中止。"""
    rid = make_release(state_path, weights=(10, 100))
    # 先由一个实例启动首批（冻结权重 10），再起 4 个实例同时中止
    run_instances([cfg("starter", state_path, rid, [{"op": "advance"}])])
    assert load_final(state_path, rid).current_weight == 10
    results = run_instances([
        cfg(f"abort-{i}", state_path, rid, [{"op": "abort", "reason": f"r{i}"}])
        for i in range(4)
    ])
    flat = [c for r in results for c in r["results"]]
    oks = [c for c in flat if c.get("ok") == "aborted"]
    errs = [c["err"] for c in flat if "err" in c]
    assert len(oks) == 1, flat
    assert set(errs) <= {"advance_conflict", "duplicate_advance"}, errs

    final = load_final(state_path, rid)
    assert final.status is ReleaseStatus.ABORTED
    assert sum(1 for e in final.events if e["type"] == "manual_abort") == 1
    assert final.current_weight == 10  # 中止冻结当前权重
    print(f"[判定] abort 生效 1 个，拒绝 {len(errs)} 个：{sorted(set(errs))}")


# --------------------------------------------------------------------------- #
# 2. 陈旧实例携带旧 version 继续操作
# --------------------------------------------------------------------------- #

def _drive_stale(state_path: Path, rid: str, ops: list[str]) -> list[dict]:
    """确定性两阶段：inst-old 先单独 started 拿到 v3；inst-new 再完成发布；
    最后 inst-old 带着陈旧的 v3 做 ops。返回打平的调用结果。"""
    # 阶段 1：old 独占 started（v2 -> v3）
    r1 = run_instances([cfg("inst-old", state_path, rid, [{"op": "advance"}])])
    assert r1[0]["results"][0]["ok"] == "started", r1
    # 阶段 2：new 在另一个进程把单批 [100] 走完门禁 -> completed（v3 -> v4）
    r2 = run_instances([cfg("inst-new", state_path, rid, [{"op": "advance"}])])
    assert r2[0]["results"][0]["ok"] == "completed", r2
    # 阶段 3：old 用陈旧版本 v3 操作
    stale_calls = [{"op": op, "expected_version": 3} for op in ops]
    r3 = run_instances([cfg("inst-old", state_path, rid, stale_calls)])
    return [c for r in (r1 + r2 + r3) for c in r["results"]]


def test_stale_advance_rejected_and_event_recorded(state_path: Path):
    """陈旧实例推进被 stale_version 拒绝：状态不回退，拒绝原因进入事件与查询。"""
    rid = make_release(state_path, weights=(100,))
    flat = _drive_stale(state_path, rid, ["advance"])
    stale = [c for c in flat if c.get("err") == "stale_version"]
    assert len(stale) == 1, flat
    assert stale[0]["status"] == "completed"  # 拒绝响应附带最新状态
    assert stale[0]["version"] == 5

    final = load_final(state_path, rid)
    assert final.status is ReleaseStatus.COMPLETED  # 绝没被写回 in_progress
    assert final.current_weight == 100
    rej = [e for e in final.events if e["type"] == "operation_rejected"]
    assert len(rej) == 1
    assert rej[0]["detail"]["reason"] == "stale_version"
    assert rej[0]["detail"]["current_status"] == "completed"
    assert rej[0]["instance_id"] == "inst-old"
    print(f"[判定] stale 拒绝事件: {rej[0]['detail']['decision']}")


def test_stale_abort_and_resume_after_rollback(state_path: Path):
    """旧实例暂停期间，发布已被另一实例的连续失败回滚：旧实例 abort/resume 不得复活它。"""
    # 手工让 inst-A 走到 paused，再让 inst-B 制造回滚较复杂；这里直接构造：
    # 单批 [100]，阈值 1：inst-A started（v3），inst-B 的门禁 unhealthy -> 立即回滚。
    rid = make_release(state_path, weights=(100,), max_failures=1)
    cfgs = [
        cfg("inst-A", state_path, rid, [{"op": "advance"}]),
    ]
    run_instances(cfgs)
    # inst-B 新进程：第一次 advance 过门禁（unhealthy，阈值 1 -> 回滚）
    run_instances([
        cfg("inst-B", state_path, rid,
            [{"op": "advance"}],
            scripted=["unhealthy"])
    ])
    mid = load_final(state_path, rid)
    assert mid.status is ReleaseStatus.ROLLED_BACK
    # inst-A 用它启动后拿到的 v3 尝试 abort / resume / advance
    results = run_instances([
        cfg("inst-A", state_path, rid, [
            {"op": "abort", "reason": "old", "expected_version": 3},
            {"op": "resume", "expected_version": 3},
            {"op": "advance", "expected_version": 3},
        ])
    ])
    flat = [c for r in results for c in r["results"]]
    assert [c["err"] for c in flat] == ["stale_version"] * 3, flat
    final = load_final(state_path, rid)
    assert final.status is ReleaseStatus.ROLLED_BACK  # 终态未被复活
    assert final.current_weight == 0 and final.active_version == "v1"
    assert sum(1 for e in final.events if e["type"] == "rollback") == 1
    assert sum(1 for e in final.events if e["type"] == "operation_rejected") == 3
    print("[判定] 回滚终态上三次陈旧操作均拒绝，权重/版本保持 rolled_back")


def test_stale_paused_instance_cannot_overwrite(state_path: Path):
    """暂停后很久的实例：别人已恢复并完成发布，它的陈旧 resume 被拒绝。"""
    rid = make_release(state_path, weights=(100,))
    # inst-A：第 1 次 advance 启动（消费第 1 个判定），第 2 次过门禁消费
    # 第 2 个判定 degraded -> paused
    run_instances([
        cfg("inst-A", state_path, rid,
            [{"op": "advance"}, {"op": "advance"}],
            scripted=["degraded", "degraded"])
    ])
    paused = load_final(state_path, rid)
    assert paused.status is ReleaseStatus.PAUSED
    paused_version = paused.version  # create + started/paused 两次 -> v3

    # inst-B 注册健康探测器，resume + advance 完成发布
    run_instances([
        cfg("inst-B", state_path, rid, [{"op": "resume"}, {"op": "advance"}])
    ])
    assert load_final(state_path, rid).status is ReleaseStatus.COMPLETED

    # inst-A 用暂停时的旧版本 resume：拒绝（共享状态已 completed）
    results = run_instances([
        cfg("inst-A", state_path, rid,
            [{"op": "resume", "expected_version": paused_version}])
    ])
    flat = [c for r in results for c in r["results"]]
    assert flat[0]["err"] == "stale_version", flat
    assert flat[0]["status"] == "completed"
    final = load_final(state_path, rid)
    assert final.status is ReleaseStatus.COMPLETED
    print(f"[判定] 暂停实例 v{paused_version} 在发布完成后 resume -> stale_version")


# --------------------------------------------------------------------------- #
# 3. 不同发布并行不互相阻塞
# --------------------------------------------------------------------------- #

def test_slow_probe_does_not_block_other_release_across_processes(state_path, tmp_path):
    """一条发布的探测卡住 3s 时，另一条发布的推进在合理时间内生效。

    复现线上现象：慢探测若占用全局提交锁，无关发布的推进会被拖着等满
    commit_timeout 并以 state_commit_busy 失败；修复后探测只占用本发布的
    发布锁，无关发布直接成功。
    """
    rid_slow = make_release(state_path, weights=(100,), name="slow-svc")
    rid_fast = make_release(state_path, weights=(100,), name="fast-svc")
    entered = tmp_path / "slow_probe_entered"

    # P1：启动 slow-svc 后再次推进 -> 进入门禁，探测卡住 3s
    p1 = _spawn(cfg("slow-inst", state_path, rid_slow,
                    [{"op": "advance"}, {"op": "advance"}],
                    fake_clock=False,
                    probe_delay_seconds=3.0,
                    probe_entered_file=str(entered)))
    # 等 P1 确实进入探测（即旧实现中"全局提交锁被慢探测占用"的窗口）
    deadline = time.monotonic() + 15
    while not entered.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert entered.exists(), "慢探测未进入"

    # P2：对完全无关的 fast-svc 连续推进到完成；commit_timeout 调小以放大回归
    t0 = time.monotonic()
    res = run_instances([cfg("fast-inst", state_path, rid_fast,
                             [{"op": "advance"}, {"op": "advance"}],
                             fake_clock=False, commit_timeout=1.0)])
    elapsed = time.monotonic() - t0

    out, err = p1.communicate(timeout=30)
    assert p1.returncode == 0, err
    slow_results = json.loads(out)["results"]

    fast = res[0]["results"]
    assert [c["ok"] for c in fast] == ["started", "completed"], fast
    assert elapsed < 2.5, f"无关发布被慢探测拖住: {elapsed:.1f}s"
    # 慢探测放行后，slow-svc 自己也正常完成
    assert [c["ok"] for c in slow_results] == ["started", "completed"], slow_results
    print(f"[判定] 慢探测卡住 3s 期间，无关发布推进到 completed 耗时 "
          f"{elapsed:.2f}s（commit_timeout=1.0s 也未触发 state_commit_busy）")


def test_different_releases_progress_in_parallel(state_path: Path):
    """30 条不同发布分两波并行推进：全部精确 started/completed 一次，无跨发布冲突。"""
    n = 30
    orch = ReleaseOrchestrator.from_path(
        str(state_path), HealthProbeRegistry(), clock=lambda: 1_000_000.0
    )
    ids = []
    for i in range(n):
        rec = orch.create_release(ReleasePlan(f"svc-{i}", f"v2-{i}", "v1", [100], 0.0))
        ids.append(rec.id)

    cfgs = [
        cfg(f"w1-{i}", state_path, rid, [{"op": "advance"}],
            tick_seconds=0.0)
        for i, rid in enumerate(ids)
    ]
    wave1 = run_instances(cfgs)
    cfgs = [
        cfg(f"w2-{i}", state_path, rid, [{"op": "advance"}],
            tick_seconds=0.0)
        for i, rid in enumerate(ids)
    ]
    wave2 = run_instances(cfgs)

    w1 = [c for r in wave1 for c in r["results"]]
    w2 = [c for r in wave2 for c in r["results"]]
    assert all(c["ok"] == "started" for c in w1), w1
    assert all(c["ok"] == "completed" for c in w2), w2
    final_orch = ReleaseOrchestrator.from_path(
        str(state_path), HealthProbeRegistry()
    )
    recs = final_orch.list_releases()
    assert len(recs) == n
    assert all(r.status is ReleaseStatus.COMPLETED for r in recs)
    assert all(r.completed_batches == 1 for r in recs)
    print(f"[判定] {n} 条不同发布两波并行全部完成，互不阻塞")


# --------------------------------------------------------------------------- #
# 4. 滚动重启后的一致性
# --------------------------------------------------------------------------- #

def test_rolling_restart_consistency(state_path: Path):
    """多轮操作间反复"重启"（每轮都是全新进程加载同一文件）：状态精确连续。"""
    rid = make_release(state_path)  # [10,50,100]
    # 每轮单独起一个全新实例（进程名不同 -> 模拟滚动重启后的新进程）
    for round_idx, expect in enumerate(
        [("started", "in_progress"), ("advanced", "in_progress"),
         ("advanced", "in_progress"), ("completed", "completed")]
    ):
        results = run_instances([
            cfg(f"restart-round-{round_idx}", state_path, rid, [{"op": "advance"}])
        ])
        c = results[0]["results"][0]
        assert c["ok"] == expect[0], (round_idx, c)
        assert c["status"] == expect[1], (round_idx, c)

    final = load_final(state_path, rid)
    assert final.status is ReleaseStatus.COMPLETED
    assert final.completed_batches == 3
    assert final.version == 6
    # 多次重启没有任何实例重复完成批次
    assert sum(1 for e in final.events if e["type"] == "release_completed") == 1
    print(f"[判定] 4 轮全新进程接力推进，最终 version=v{final.version}，批次无重复")


def test_restart_after_paused_then_other_instance_aborts(state_path: Path):
    """实例重启回来前发布已被中止：旧进度不能覆盖，查询结果显示 aborted 与拒绝事件。"""
    rid = make_release(state_path, weights=(100,))
    run_instances([cfg("A", state_path, rid, [{"op": "advance"}])])
    pre = load_final(state_path, rid)
    v_before = pre.version
    # 另一个实例直接中止
    run_instances([cfg("B", state_path, rid, [{"op": "abort", "reason": "stop"}])])
    # A"重启回来"（全新进程），携带重启前版本继续推进 -> stale
    results = run_instances([
        cfg("A-restarted", state_path, rid,
            [{"op": "advance", "expected_version": v_before}])
    ])
    c = results[0]["results"][0]
    assert c["err"] == "stale_version", c
    assert c["status"] == "aborted"
    final = load_final(state_path, rid)
    assert final.status is ReleaseStatus.ABORTED
    assert any(
        e["type"] == "operation_rejected" and e["detail"]["op"] == "advance"
        for e in final.events
    )
    print(f"[判定] 重启实例 v{v_before} 对已中止发布推进 -> stale_version(aborted)")


# --------------------------------------------------------------------------- #
# 5. 高压并发下的状态完整性（无半截状态、无损坏、无卡死）
# --------------------------------------------------------------------------- #

def test_high_concurrency_state_integrity(state_path: Path):
    """多进程混合推进/查询 + 多个"重启"实例反复加载：文件始终完整、不变质。"""
    n_releases = 24
    orch = ReleaseOrchestrator.from_path(
        str(state_path), HealthProbeRegistry(), clock=lambda: 1_000_000.0
    )
    ids = []
    for i in range(n_releases):
        rec = orch.create_release(
            ReleasePlan(f"stress-{i}", f"v2-{i}", "v1", [25, 50, 100], 0.0)
        )
        ids.append(rec.id)

    # 每发布 2 个进程同时"推进到完成"：每个 advance 都带 retry_until_effective，
    # 即对 advance_conflict/观察期未满/锁忙做退避重试，直到本次生效或到达终态。
    # 两个进程从始至终争抢同一发布（高冲突压力），但每条发布只允许 4 次生效推进
    # （started/advanced/advanced/completed），多出来的调用必须全部撞终态而返回。
    cfgs = []
    for rid in ids:
        for i in range(2):
            calls = (
                [{"op": "get"}]
                + [{"op": "advance", "retry_until_effective": True} for _ in range(4)]
                + [{"op": "get"}]
            )
            cfgs.append(cfg(f"stress-{rid[:4]}-{i}", state_path, rid, calls,
                            tick_seconds=0.0))
    import time as _time
    t0 = _time.monotonic()
    results = run_instances(cfgs)
    elapsed = _time.monotonic() - t0

    # 每条发布必须最终 completed，且恰好 3 次 traffic_switched / 1 次 completed
    final_orch = ReleaseOrchestrator.from_path(
        str(state_path), HealthProbeRegistry()
    )
    effective = 0
    retries = 0
    for r in results:
        for c in r["results"]:
            if "ok" in c and c["ok"] in ("started", "advanced", "completed"):
                effective += 1
                retries += c.get("tries", 1) - 1
    for rid in ids:
        rec = final_orch.get_release(rid)
        assert rec.status is ReleaseStatus.COMPLETED, rec.id
        assert rec.completed_batches == 3
        assert rec.current_weight == 100 and rec.active_version == f"v2-{ids.index(rid)}"
        assert sum(1 for e in rec.events if e["type"] == "traffic_switched") == 3
        assert sum(1 for e in rec.events if e["type"] == "release_completed") == 1
        assert [e["seq"] for e in rec.events] == list(range(1, len(rec.events) + 1))
    assert effective == n_releases * 4  # 每发布恰好 4 次生效，其余全部撞终态

    # 原始 JSON 必须可解析且 schema_version 完好（没有任何半截写）
    raw = json.loads(state_path.read_text())
    assert raw["schema_version"] == 1
    assert len(raw["releases"]) == n_releases
    assert elapsed < 45.0, f"接口长时间卡死: {elapsed:.1f}s"
    total_calls = sum(len(r["results"]) for r in results)
    print(f"[判定] {n_releases} 条发布 × 2 进程争抢推进：生效 {effective} 次，"
          f"冲突/等待重试 {retries} 次，记录调用 {total_calls} 条，{elapsed:.1f}s，"
          f"全部精确完成且状态文件完整")


# --------------------------------------------------------------------------- #
# 6. 同进程多实例（快速、确定性）：版本单调与旧快照保护
# --------------------------------------------------------------------------- #

def _two_instances(state_path: Path, clock):
    reg1, reg2 = HealthProbeRegistry(), HealthProbeRegistry()
    a = ReleaseOrchestrator.from_path(str(state_path), reg1, clock=clock,
                                      instance_id="local-A")
    b = ReleaseOrchestrator.from_path(str(state_path), reg2, clock=clock,
                                      instance_id="local-B")
    return a, b, reg1, reg2


def test_version_monotonic_and_cache_follows_shared_state(state_path, clock):
    """A 改完 B 立刻可见；version 每次生效变更 +1，B 不能用内存旧视图覆盖。"""
    import asyncio
    from release_orchestrator.errors import StaleVersionError
    from release_orchestrator.health import AlwaysHealthyProbe

    a, b, reg1, reg2 = _two_instances(state_path, clock)
    reg1.register  # noqa
    rid = a.create_release(ReleasePlan("v", "v2", "v1", [50, 100], 60)).id
    reg1.register(rid, AlwaysHealthyProbe())
    reg2.register(rid, AlwaysHealthyProbe())

    assert a.get_release(rid).version == 2
    assert b.get_release(rid).version == 2  # B 查询即读到盘上最新

    asyncio.run(a.advance(rid))  # started -> v3
    view_b = b.get_release(rid)
    assert view_b.status is ReleaseStatus.IN_PROGRESS
    assert view_b.version == 3

    clock.advance(61)
    asyncio.run(b.advance(rid))  # B 推进到第 2 批 -> v4
    clock.advance(61)
    asyncio.run(b.advance(rid))  # B 完成 -> v5
    assert a.get_release(rid).version == 5

    # A 拿着 v3 做 resume（当前已 completed，且版本陈旧）-> stale，先撞版本检查
    with pytest.raises(StaleVersionError):
        asyncio.run(a.resume(rid, expected_version=3))
    rec = b.get_release(rid)
    assert rec.status is ReleaseStatus.COMPLETED
    assert rec.version == 6  # 拒绝事件也让 version +1
    assert rec.events[-1]["type"] == "operation_rejected"


def test_concurrent_conflict_persists_nothing(state_path, clock):
    """抢到发布锁失败的调用不写盘、不加事件、version 不变。"""
    import asyncio
    from release_orchestrator.errors import AdvanceConflictError
    from release_orchestrator.health import AlwaysHealthyProbe

    a, b, reg1, reg2 = _two_instances(state_path, clock)
    rid = a.create_release(ReleasePlan("v", "v2", "v1", [10, 100], 60)).id
    reg1.register(rid, AlwaysHealthyProbe())
    reg2.register(rid, AlwaysHealthyProbe())

    async def race():
        return await asyncio.gather(
            a.advance(rid), b.advance(rid), return_exceptions=True
        )

    results = asyncio.run(race())
    oks = [r for r in results if not isinstance(r, Exception)]
    rejected = [r for r in results if isinstance(r, Exception)]
    assert len(oks) == 1, results
    assert len(rejected) == 1
    # 同进程两个编排器实例：另一个调用必须以可区分方式未生效
    # （抢锁冲突，或拿到锁后判观察期未满），且绝不产生第二次切换
    assert rejected[0].error_code in ("advance_conflict", "duplicate_advance")
    rec = a.get_release(rid)
    assert rec.status is ReleaseStatus.IN_PROGRESS
    # 纯抢锁冲突不留痕（v3）；观察期未满拒绝会落 advance_rejected（v4），
    # 两种路径都只允许一次切流。
    assert rec.version in (3, 4)
    types = [e["type"] for e in rec.events]
    assert types[:2] == ["release_created", "traffic_switched"]
    assert sum(t == "traffic_switched" for t in types) == 1
    assert set(types[2:]) <= {"advance_rejected"}
