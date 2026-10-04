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
    """4 个进程同时对同一发布连续推进：每次切换只发生一次，其余调用明确未生效。"""
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

    # 3 批权重：started + advanced + advanced + completed，共 4 次生效。
    # 生效动作落在哪些进程上是非确定性的（取决于进程调度谁抢到每次锁），
    # 但按提交后的 version 排序后，动作序列必须严格等于状态机顺序。
    oks_sorted = sorted(oks, key=lambda c: c["version"])
    actions = [c["ok"] for c in oks_sorted]
    assert [c["version"] for c in oks_sorted] == [3, 4, 5, 6], oks_sorted
    assert actions == ["started", "advanced", "advanced", "completed"], actions
    # 其余 12 次全部明确拒绝（冲突 / 观察期未满 / 终态非法转换），没有"半生效"
    assert len(rejects) == 12
    assert {c["err"] for c in rejects} <= {
        "advance_conflict", "duplicate_advance", "illegal_transition"
    }

    final = load_final(state_path, rid)
    assert final.status is ReleaseStatus.COMPLETED
    assert final.completed_batches == 3
    assert final.current_weight == 100
    assert final.active_version == "v2"
    # 每次切流只发生一次，完成事件唯一，事件 seq 连续无重复
    assert sum(1 for e in final.events if e["type"] == "traffic_switched") == 3
    assert sum(1 for e in final.events if e["type"] == "release_completed") == 1
    assert [e["seq"] for e in final.events] == list(range(1, len(final.events) + 1))
    # create(v2) + 4 次生效推进(v3..v6)；进入状态机的状态性拒绝（观察期未满 /
    # 终态拒绝）会各自留痕并让 version 再 +1，纯抢锁冲突(advance_conflict)不动版本。
    traced_rejections = [
        e for e in final.events
        if e["type"] in ("advance_rejected", "operation_rejected")
    ]
    assert final.version == 6 + len(traced_rejections), (
        final.version, [e["type"] for e in traced_rejections]
    )
    assert all(
        e["detail"].get("current_status") in ("in_progress", "completed")
        for e in traced_rejections if e["type"] == "operation_rejected"
    )
    print(f"[判定] 生效 {len(oks)} 次（{actions}），拒绝 {len(rejects)} 次"
          f"（其中状态性留痕 {len(traced_rejections)} 次），最终 version=v{final.version}")


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
# 7. 跨进程：一条发布探测变慢不连累其他发布；同发布慢探测期间仍是单赢家
# --------------------------------------------------------------------------- #


def test_cross_process_slow_probe_other_release_unblocked(state_path: Path):
    """slow 发布的门禁探测阻塞 1.5s 时，另一进程对 fast 发布的推进立即生效。

    旧实现把全局提交锁整段握过慢探测，fast 进程会排队 1.5s（甚至 503）；
    新实现全局锁只在毫秒级提交阶段持有，fast 应当亚秒级 started/completed。
    """
    import time as _time

    # 两条发布：slow（2 批，真实时间观察期 0）、fast（单批）
    slow_id = make_release(state_path, weights=(10, 100), name="slow-svc")
    fast_id = make_release(state_path, weights=(100,), name="fast-svc")
    # slow 先 started（独立进程，避免和后续慢调用同进程）
    run_instances([cfg("slow-start", state_path, slow_id, [{"op": "advance"}],
                       fake_clock=False)])
    assert load_final(state_path, slow_id).current_weight == 10

    # slow 进程：第二次 advance 过门禁，探测固定阻塞 1.5s
    slow_cfg = cfg(
        "slow-probe", state_path, slow_id, [{"op": "advance"}], fake_clock=False,
        slow_probe={"delay": 1.5},
    )
    # fast 进程：对完全不同的发布先 started 再 completed（两次提交）
    fast_cfg = cfg(
        "fast-ops", state_path, fast_id,
        [{"op": "advance"}, {"op": "advance"}], fake_clock=False,
    )

    t0 = _time.monotonic()
    slow_proc, fast_proc = _spawn(slow_cfg), _spawn(fast_cfg)
    fast_out, fast_err = fast_proc.communicate(timeout=30)
    fast_elapsed = _time.monotonic() - t0
    assert fast_proc.returncode == 0, fast_err
    slow_out, slow_err = slow_proc.communicate(timeout=30)
    assert slow_proc.returncode == 0, slow_err

    fast_payload = json.loads(fast_out)
    assert "fatal" not in fast_payload, fast_payload
    fast_calls = fast_payload["results"]
    codes = [c.get("err") for c in fast_calls]
    assert not any(c in ("state_commit_busy", "advance_conflict") for c in codes), fast_calls
    assert [c["ok"] for c in fast_calls] == ["started", "completed"], fast_calls
    # 关键：fast 没有等待 slow 的 1.5s 探测，也没被 503 拒绝
    assert fast_elapsed < 0.8, f"无关发布被慢探测拖住 {fast_elapsed:.2f}s"

    slow_payload = json.loads(slow_out)
    assert slow_payload["results"][0]["ok"] == "advanced", slow_payload
    final_slow = load_final(state_path, slow_id)
    final_fast = load_final(state_path, fast_id)
    # slow 的慢门禁通过后：完成第 1 批并切到第 2 批权重 100（尚未再过门禁，不完成）
    assert final_slow.status is ReleaseStatus.IN_PROGRESS
    assert final_slow.current_weight == 100 and final_slow.completed_batches == 1
    assert final_fast.status is ReleaseStatus.COMPLETED
    print(f"[判定] 跨进程 slow 探测 1.5s 期间，fast 发布两次操作 {fast_elapsed*1000:.0f}ms "
          "完成，无排队无 503")


def test_cross_process_same_release_conflict_during_slow_probe(state_path: Path):
    """slow 进程卡在同发布门禁时，其他进程对该发布的操作立即 advance_conflict。"""
    rid = make_release(state_path, weights=(10, 100), name="same-slow")
    run_instances([cfg("starter", state_path, rid, [{"op": "advance"}],
                       fake_clock=False)])

    slow_cfg = cfg("slow-probe", state_path, rid, [{"op": "advance"}],
                   fake_clock=False, slow_probe={"delay": 1.5})
    rival_cfgs = [
        cfg(f"rival-{i}", state_path, rid, [{"op": "advance"}], fake_clock=False)
        for i in range(3)
    ]
    slow_proc = _spawn(slow_cfg)
    # 给慢调用一点时间真正进入探测，再同时起 3 个竞争进程
    import time as _time
    _time.sleep(0.3)
    rival_results = run_instances(rival_cfgs)
    slow_out, slow_err = slow_proc.communicate(timeout=30)
    assert slow_proc.returncode == 0, slow_err

    flat = [c for r in rival_results for c in r["results"]]
    assert all(c.get("err") == "advance_conflict" for c in flat), flat
    slow_payload = json.loads(slow_out)
    assert slow_payload["results"][0]["ok"] == "advanced", slow_payload

    final = load_final(state_path, rid)
    assert final.current_batch == 2 and final.current_weight == 100
    assert sum(1 for e in final.events if e["type"] == "traffic_switched") == 2
    print("[判定] 慢探测期间 3 个跨进程竞争调用全部立即 advance_conflict，切流仍只一次")


# --------------------------------------------------------------------------- #
# 8. 同进程多实例（快速、确定性）：版本单调与旧快照保护
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
