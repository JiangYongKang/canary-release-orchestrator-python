"""重复提交、非法状态转换、人工中止链路的拒绝结果必须可区分。"""

from __future__ import annotations

import pytest

from canary.errors import (
    DuplicateSubmissionError,
    IllegalTransitionError,
    ManualAbortError,
)
from canary.models import ReleasePlan, ReleaseStatus


def plan(rid: str = "rel-x", **kw) -> ReleasePlan:
    base = dict(
        release_id=rid,
        candidate_version="2.0.0",
        stable_version="1.0.0",
        batch_weights=[50, 100],
        min_observation_seconds=0,
    )
    base.update(kw)
    return ReleasePlan(**base)


def test_duplicate_submission_is_distinct(orch):
    orch.create_release(plan())
    with pytest.raises(DuplicateSubmissionError) as ei:
        orch.create_release(plan())
    assert ei.value.code == "duplicate_submission"
    assert ei.value.http_status == 409
    print(f"\n重复提交被拒绝: code={ei.value.code} msg={ei.value.message}")


def test_invalid_plan_rejected():
    # 权重必须严格递增且末项 100；候选/稳定版本不能相同
    with pytest.raises(Exception):
        plan("a", batch_weights=[80, 20, 100])
    with pytest.raises(Exception):
        plan("b", batch_weights=[50, 80])
    with pytest.raises(Exception):
        plan("c", candidate_version="1.0.0")
    print("\n非法计划（权重非递增/末项非100/版本相同）均被拒绝")


def test_advance_unknown_release(orch):
    with pytest.raises(IllegalTransitionError) as ei:
        orch.advance("nope")
    assert ei.value.code == "illegal_transition"
    print(f"\n推进不存在的发布: {ei.value.message}")


def test_terminal_states_reject_actions(orch, gate):
    orch.create_release(plan())
    gate.inject_healthy("rel-x")
    gate.inject_healthy("rel-x")
    orch.advance("rel-x")  # batch0 done
    orch.advance("rel-x")  # completed
    assert orch.get("rel-x").status == ReleaseStatus.COMPLETED

    # 完成后重复推进 / 暂停 / 回滚 / 再中止 -> 全部非法转换
    for action in ("advance", "pause", "rollback"):
        with pytest.raises(IllegalTransitionError) as ei:
            getattr(orch, action)("rel-x")
        assert ei.value.code == "illegal_transition"
        print(f"COMPLETED 状态执行 {action} 被拒绝: {ei.value.message}")

    with pytest.raises(IllegalTransitionError):
        orch.abort("rel-x")


def test_pause_resume_transitions(orch):
    orch.create_release(plan("rel-p"))
    # pending 状态直接 pause 非法
    with pytest.raises(IllegalTransitionError) as ei:
        orch.pause("rel-p")
    assert ei.value.code == "illegal_transition"
    print(f"\nPENDING 状态 pause 被拒绝: {ei.value.message}")

    orch.advance("rel-p")  # 零观察期 + 无注入 -> 默认探针健康 -> batch done
    r = orch.pause("rel-p")
    assert r.state.status == ReleaseStatus.PAUSED
    # 重复 pause 可区分拒绝
    with pytest.raises(IllegalTransitionError):
        orch.pause("rel-p")
    # progressing 才能 pause；resume 只允许从 paused
    r = orch.resume("rel-p")
    assert r.state.status == ReleaseStatus.PROGRESSING
    with pytest.raises(IllegalTransitionError) as ei:
        orch.resume("rel-p")
    print(f"PROGRESSING 状态 resume 被拒绝: {ei.value.message}")


def test_manual_abort_chain_is_consistent(orch):
    orch.create_release(plan("rel-a"))
    orch.advance("rel-a")  # batch0 started+gate(healthy default) -> done, weight=50
    assert orch.get("rel-a").current_weight == 50

    r = orch.abort("rel-a", reason="需求撤销")
    assert r.action == "aborted"
    state = orch.get("rel-a")
    assert state.status == ReleaseStatus.ABORTED
    assert state.current_weight == 0
    assert state.active_version == "1.0.0"
    print(f"\n人工中止: status=ABORTED 权重候选0%/稳定100% 依据={state.rollback_reason}")

    # 中止后再推进 -> 人工中止异常链路（区别于普通非法转换）
    with pytest.raises(ManualAbortError) as ei:
        orch.advance("rel-a")
    assert ei.value.code == "manual_abort"
    print(f"ABORTED 状态推进走人工中止链路: {ei.value.message}")

    # 重复 abort 幂等
    r2 = orch.abort("rel-a")
    assert r2.action == "already_aborted"
    print("重复 abort 幂等，状态未改变")
