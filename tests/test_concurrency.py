"""并发安全：同一发布被并发推进时，只能产生一个有效推进结果。

存储层用进程内锁 + fcntl 文件锁串行化读改写事务，因此：
- 恰好一个批次被执行，completed_batches 不重复、不丢失、不回退；
- 其余调用走各自可区分的拒绝链路（观察期 / 非法状态）；
- 最终权重与状态一致。
"""

from __future__ import annotations

import threading
from collections import Counter

from canary.errors import OrchestratorError
from canary.models import ReleasePlan, ReleaseStatus


def plan(rid: str = "rel-conc") -> ReleasePlan:
    return ReleasePlan(
        release_id=rid,
        candidate_version="2.0.0",
        stable_version="1.0.0",
        batch_weights=[25, 50, 75, 100],
        min_observation_seconds=0.1,
    )


def test_concurrent_advance_single_effective(orch, gate):
    rid = "rel-conc"
    orch.create_release(plan())

    # 门闩：先让所有线程就绪，再同时释放，最大化竞争
    start = threading.Barrier(12)
    results: Counter = Counter()
    effective = []
    rejected: list[OrchestratorError] = []
    lock = threading.Lock()

    def worker(i: int):
        try:
            start.wait()
        except threading.BrokenBarrierError:
            return
        try:
            r = orch.advance(rid)
            with lock:
                results[r.action] += 1
                effective.append((i, r.action, r.state.current_weight, r.state.completed_batches))
        except OrchestratorError as exc:
            with lock:
                results[exc.code] += 1
                rejected.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state = orch.get(rid)
    print(f"\n并发推进结果分布: {dict(results)}")
    print(f"最终: status={state.status.value} weight={state.current_weight}% "
          f"completed_batches={state.completed_batches}")

    # 只有一个调用真正把批次从“未启动”推进到“已启动/观察中”
    assert results.get("batch_started", 0) == 1
    assert state.completed_batches == 0  # 观察期 0.1s，门禁尚未执行
    assert state.current_weight == 25
    # 其余全部是可区分的观察期拒绝，没有状态丢失/重复切换
    assert results.get("observation_pending", 0) == 11
    assert all(exc.remaining_seconds is not None for exc in rejected)


def test_concurrent_zero_obs_finishes_once(orch, gate):
    """零观察期 + 多线程同时推进：整个发布只被完整推进一次，终态唯一。"""
    rid = "rel-once"
    orch.create_release(ReleasePlan(
        release_id=rid, candidate_version="2.0.0", stable_version="1.0.0",
        batch_weights=[50, 100], min_observation_seconds=0,
    ))
    # 每个并发调用都可能命中门禁，注入足够多的健康样本（多余样本被忽略）
    for _ in range(40):
        gate.inject_healthy(rid, "健康")

    barrier = threading.Barrier(8)
    outcomes: Counter = Counter()
    lock = threading.Lock()

    def worker():
        barrier.wait()
        try:
            r = orch.advance(rid)
            with lock:
                outcomes[r.action] += 1
        except OrchestratorError as exc:
            with lock:
                outcomes[exc.code] += 1

    # 发起远多于批次数的并发推进
    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state = orch.get(rid)
    print(f"\n零观察期并发推进分布: {dict(outcomes)}")
    print(f"最终: status={state.status.value} completed={state.completed_batches}/2 weight={state.current_weight}%")

    # 每个批次恰好完成一次
    assert outcomes.get("batch_completed", 0) == 1
    assert outcomes.get("completed", 0) == 1
    assert state.completed_batches == 2
    assert state.status == ReleaseStatus.COMPLETED
    assert state.active_version == "2.0.0"
    # 多出来的调用全部得到终态拒绝，而不是重复切换
    assert outcomes.get("illegal_transition", 0) == 6
