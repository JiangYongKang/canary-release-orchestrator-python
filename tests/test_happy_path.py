"""正常推进：批次权重逐步切换、观察期两阶段、完成后候选转正。"""

from __future__ import annotations

import time

import pytest

from canary.models import ReleasePlan, ReleaseStatus
from canary.errors import ObservationPendingError


def make_plan(rid: str = "rel-happy", obs: float = 0.02) -> ReleasePlan:
    return ReleasePlan(
        release_id=rid,
        candidate_version="2.0.0",
        stable_version="1.0.0",
        batch_weights=[20, 60, 100],
        min_observation_seconds=obs,
    )


def test_full_progression(orch, gate):
    print("\n输入计划: candidate=2.0.0 stable=1.0.0 weights=[20,60,100] obs=0.02s")
    orch.create_release(make_plan())

    for i, weight in enumerate([20, 60, 100]):
        r = orch.advance("rel-happy")
        assert r.action == "batch_started"
        assert r.state.current_weight == weight
        assert r.weight_split.stable_weight == 100 - weight
        print(f"批次{i} 启动: 候选 {weight}% / 稳定 {100 - weight}%")

        # 观察期内推进 -> 可区分拒绝，权重不得变化
        with pytest.raises(ObservationPendingError) as ei:
            orch.advance("rel-happy")
        assert ei.value.code == "observation_pending"
        assert ei.value.remaining_seconds is not None
        assert orch.get("rel-happy").current_weight == weight
        print(f"批次{i} 观察期内推进被拒绝: {ei.value.message}")
        time.sleep(0.03)

        gate.inject_healthy("rel-happy", reason=f"batch{i} 指标正常")
        r = orch.advance("rel-happy")

        state = orch.get("rel-happy")
        if i < 2:
            assert r.action == "batch_completed"
            assert state.completed_batches == i + 1
            assert state.status == ReleaseStatus.PROGRESSING
            print(f"批次{i} 门禁 CONTINUE -> 完成 {state.completed_batches}/3，判定依据: {r.gate.reason}")
        else:
            assert r.action == "completed"
            assert state.status == ReleaseStatus.COMPLETED
            assert state.current_weight == 100
            assert state.active_version == "2.0.0"
            assert r.weight_split.stable_weight == 0
            print("最终: 状态 COMPLETED，对外版本=2.0.0，候选 100%")


def test_zero_observation_single_call(orch, gate):
    """观察期为 0：同一次 advance 完成切权重 + 门禁。"""
    orch.create_release(make_plan("rel-zero", obs=0))
    gate.inject_healthy("rel-zero")
    r = orch.advance("rel-zero")
    assert r.action == "batch_completed"
    assert r.state.completed_batches == 1
    print("\n零观察期: 单次 advance 即完成批次 0", r.state.current_weight)
