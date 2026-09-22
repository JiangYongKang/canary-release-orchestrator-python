"""发布编排核心：受控推进、健康门禁、自动回滚。

并发安全：所有读改写都在 store 的独占事务（进程锁 + flock）中完成，
同一 release 的并发推进只有一个调用能真正推进批次，其余得到可区分的拒绝
（观察期未到 / 非法状态 / 重复）。

持久化即去重：每个批次在门禁判定前落盘 ``started_at``、判定后落盘结果，
重启恢复时以 ``completed_batches`` 为准，绝不重复执行已完成批次。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from .errors import (
    DuplicateSubmissionError,
    IllegalTransitionError,
    ManualAbortError,
    ObservationPendingError,
    OrchestratorError,
)
from .health import HealthGate
from .models import (
    AdvanceResult,
    BatchRecord,
    FailureKind,
    ReleasePlan,
    ReleaseState,
    ReleaseStatus,
    WeightSplit,
    utcnow,
)
from .store import JsonStateStore

logger = logging.getLogger("canary.orchestrator")


class ReleaseOrchestrator:
    def __init__(
        self,
        store: JsonStateStore,
        gate: HealthGate | None = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.gate = gate or HealthGate()
        self._monotonic = monotonic

    # ============================================================== 创建计划

    def create_release(self, plan: ReleasePlan) -> ReleaseState:
        def tx(states: dict[str, ReleaseState]) -> ReleaseState:
            if plan.release_id in states:
                raise DuplicateSubmissionError(
                    f"发布 {plan.release_id!r} 已存在，重复提交被拒绝",
                    release_id=plan.release_id,
                )
            state = ReleaseState(
                release_id=plan.release_id,
                candidate_version=plan.candidate_version,
                stable_version=plan.stable_version,
                batch_weights=list(plan.batch_weights),
                min_observation_seconds=plan.min_observation_seconds,
                status=ReleaseStatus.PENDING,
                completed_batches=0,
                current_weight=0,
                active_version=plan.stable_version,
                batches=[BatchRecord(index=i, weight=w) for i, w in enumerate(plan.batch_weights)],
            )
            states[plan.release_id] = state
            logger.info(
                "[create] release=%s candidate=%s stable=%s weights=%s 状态 PENDING",
                plan.release_id, plan.candidate_version, plan.stable_version, plan.batch_weights,
            )
            return state

        return self.store.transaction(tx)

    # ============================================================== 受控推进

    def advance(self, release_id: str) -> AdvanceResult:
        # pending_error 在事务中构造、事务提交后抛出
        pending_error: ObservationPendingError | None = None

        def tx(states: dict[str, ReleaseState]):
            state = self._require(states, release_id)

            if state.status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
                raise IllegalTransitionError(
                    f"发布已处于终态 {state.status.value}，拒绝重复推进",
                    release_id=release_id,
                )
            if state.status == ReleaseStatus.ABORTED:
                raise ManualAbortError(
                    "发布已被人工中止（终态），拒绝推进",
                    release_id=release_id,
                )

            # 当前要推进的批次（已完成的批次不会重复执行）
            idx = state.completed_batches
            if idx >= len(state.batch_weights):
                raise IllegalTransitionError(
                    "所有批次均已完成，没有可推进的批次", release_id=release_id
                )
            batch = state.batches[idx]

            # PAUSED -> PROGRESSING（人工恢复后，批次保持已切好的权重继续观察）
            if state.status == ReleaseStatus.PAUSED:
                state.status = ReleaseStatus.PROGRESSING
                state.updated_at = utcnow()
                logger.info("[advance] release=%s 状态 PAUSED -> PROGRESSING", release_id)

            if batch.started_at is None:
                # 阶段一：本批次尚未启动 -> 切到本批次权重并开始观察，然后落盘
                if state.status != ReleaseStatus.PROGRESSING:
                    # PENDING -> PROGRESSING
                    if not state.status.can_transition_to(ReleaseStatus.PROGRESSING):
                        raise IllegalTransitionError(
                            f"状态 {state.status.value} 不允许推进到 progressing",
                            release_id=release_id,
                        )
                    state.status = ReleaseStatus.PROGRESSING
                    logger.info("[advance] release=%s 状态 -> PROGRESSING", release_id)
                batch.started_at = utcnow()
                state.current_weight = batch.weight
                state.updated_at = utcnow()
                logger.info(
                    "[advance] release=%s 批次 %d 开始，候选流量 -> %d%%，进入观察期(>=%ss)",
                    release_id, idx, batch.weight, state.min_observation_seconds,
                )
                # 观察期为 0 时，同一次调用直接执行门禁，便于本地一步验证
                if state.min_observation_seconds <= 0:
                    return self._run_gate(states, state, idx)
                return ("observing", state)

            # 阶段二：批次已启动，检查最小观察期
            elapsed = (utcnow() - batch.started_at).total_seconds()
            if elapsed < state.min_observation_seconds:
                nonlocal pending_error
                pending_error = ObservationPendingError(
                    f"批次 {idx} 仍在最小观察期：已观察 {elapsed:.3f}s，"
                    f"需 {state.min_observation_seconds:g}s",
                    release_id=release_id,
                    remaining_seconds=state.min_observation_seconds - elapsed,
                )
                return ("pending", state)

            # 观察期满，执行门禁
            return self._run_gate(states, state, idx)

        outcome = self.store.transaction(tx)

        kind = outcome[0]
        if kind == "pending":
            assert pending_error is not None
            raise pending_error

        state: ReleaseState = outcome[1]
        if kind == "observing":
            return AdvanceResult(
                release_id=release_id, action="batch_started",
                message=f"批次已启动，候选流量切换到 {state.current_weight}%，"
                        f"需观察至少 {state.min_observation_seconds:g}s 后再次调用 advance",
                state=state, weight_split=self._split(state),
            )
        if kind == "gate_error":
            gate_err: OrchestratorError = outcome[2]
            # 回滚已落盘；把回滚后的状态附在可识别异常上抛出
            gate_err.state_after = state  # type: ignore[attr-defined]
            gate_err.rolled_back = True  # type: ignore[attr-defined]
            logger.error(
                "[advance] release=%s 健康异常链路=%s，已自动回滚：%s",
                release_id, gate_err.code, gate_err.message,
            )
            raise gate_err
        if kind == "rolled_back":
            return AdvanceResult(
                release_id=release_id, action="rolled_back",
                message=f"门禁判定回滚：{state.rollback_reason}",
                state=state, gate=outcome[2], weight_split=self._split(state),
                rolled_back=True,
            )
        if kind == "paused":
            return AdvanceResult(
                release_id=release_id, action="paused",
                message="门禁判定暂停，等待人工处置（resume / rollback / abort）",
                state=state, gate=outcome[2], weight_split=self._split(state),
            )
        if kind == "completed":
            return AdvanceResult(
                release_id=release_id, action="completed",
                message="发布完成，候选版本已接管全部流量",
                state=state, gate=outcome[2], weight_split=self._split(state),
            )
        return AdvanceResult(
            release_id=release_id, action="batch_completed",
            message=f"批次完成，已完成 {state.completed_batches}/{len(state.batch_weights)}，"
                    "再次调用 advance 进入下一批次",
            state=state, gate=outcome[2], weight_split=self._split(state),
        )

    def _run_gate(self, states: dict, state: ReleaseState, idx: int):
        """观察期满后的门禁判定（必须在已持有的事务内调用）。"""
        batch = state.batches[idx]
        try:
            evaluation = self.gate.evaluate(state.release_id, state.candidate_version)
        except OrchestratorError as gate_err:
            self._apply_rollback(
                state,
                reason=gate_err.message,
                failure_kind=self._infer_failure(gate_err),
            )
            return ("gate_error", state, gate_err)

        state.last_gate = evaluation
        batch.gate_decision = evaluation.decision
        batch.observed_at = utcnow()
        logger.info(
            "[advance] release=%s 批次 %d 门禁=%s 依据=%s",
            state.release_id, idx, evaluation.decision.value, evaluation.reason,
        )

        if evaluation.decision.value == "rollback":
            # 注入式探针直接给回滚结论（不走异常链时的兼容路径）
            self._apply_rollback(state, reason=evaluation.reason,
                                 failure_kind=FailureKind.CONSECUTIVE_FAILURE)
            return ("rolled_back", state, evaluation)

        if evaluation.decision.value == "pause":
            state.status = ReleaseStatus.PAUSED
            state.updated_at = utcnow()
            logger.info("[advance] release=%s 状态 -> PAUSED，权重保持 %d%%",
                        state.release_id, state.current_weight)
            return ("paused", state, evaluation)

        # CONTINUE：完成本批次
        batch.completed = True
        state.completed_batches = idx + 1
        if state.completed_batches >= len(state.batch_weights):
            state.status = ReleaseStatus.COMPLETED
            state.current_weight = 100
            state.active_version = state.candidate_version
            state.updated_at = utcnow()
            logger.info(
                "[advance] release=%s 全部批次完成，状态 -> COMPLETED，对外版本=%s",
                state.release_id, state.active_version,
            )
            return ("completed", state, evaluation)

        # 批次间保持 PROGRESSING，下一批次尚未启动（started_at=None），
        # 下一次 advance 切到下一档权重；已完成批次绝不重复执行。
        state.updated_at = utcnow()
        logger.info(
            "[advance] release=%s 批次 %d 完成（%d/%d），等待下一次推进",
            state.release_id, idx, state.completed_batches, len(state.batch_weights),
        )
        return ("batch_done", state, evaluation)

    # ============================================================== 人工动作

    def pause(self, release_id: str) -> AdvanceResult:
        def tx(states):
            state = self._require(states, release_id)
            if state.status == ReleaseStatus.PAUSED:
                raise IllegalTransitionError("发布已处于暂停状态", release_id=release_id)
            if state.status != ReleaseStatus.PROGRESSING:
                raise IllegalTransitionError(
                    f"状态 {state.status.value} 不允许人工暂停", release_id=release_id
                )
            state.status = ReleaseStatus.PAUSED
            state.updated_at = utcnow()
            logger.info("[pause] release=%s 人工暂停，权重保持 %d%%",
                        release_id, state.current_weight)
            return state

        state = self.store.transaction(tx)
        return AdvanceResult(release_id=release_id, action="paused_manually",
                             message="已人工暂停", state=state, weight_split=self._split(state))

    def resume(self, release_id: str) -> AdvanceResult:
        def tx(states):
            state = self._require(states, release_id)
            if state.status != ReleaseStatus.PAUSED:
                raise IllegalTransitionError(
                    f"仅 paused 状态可恢复，当前为 {state.status.value}", release_id=release_id
                )
            state.status = ReleaseStatus.PROGRESSING
            # 恢复后重新起观察期
            idx = state.completed_batches
            if idx < len(state.batch_weights):
                state.batches[idx].started_at = utcnow()
            state.updated_at = utcnow()
            logger.info("[resume] release=%s 人工恢复推进", release_id)
            return state

        state = self.store.transaction(tx)
        return AdvanceResult(release_id=release_id, action="resumed",
                             message="已恢复为推进中，再次调用 advance 执行门禁",
                             state=state, weight_split=self._split(state))

    def rollback(self, release_id: str, *, reason: str = "人工回滚") -> AdvanceResult:
        def tx(states):
            state = self._require(states, release_id)
            if state.status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
                raise IllegalTransitionError(
                    f"状态 {state.status.value} 为终态，不能再回滚", release_id=release_id
                )
            if state.status == ReleaseStatus.ABORTED:
                raise ManualAbortError("发布已中止，无需再回滚", release_id=release_id)
            self._apply_rollback(state, reason=reason, failure_kind=FailureKind.MANUAL_ABORT)
            logger.warning("[rollback] release=%s 人工回滚：%s", release_id, reason)
            return state

        state = self.store.transaction(tx)
        return AdvanceResult(release_id=release_id, action="rolled_back_manually",
                             message=f"已回滚至 {state.stable_version}：{reason}",
                             state=state, weight_split=self._split(state), rolled_back=True)

    def abort(self, release_id: str, *, reason: str = "人工中止") -> AdvanceResult:
        def tx(states):
            state = self._require(states, release_id)
            if state.status == ReleaseStatus.ABORTED:
                # 幂等：重复中止不改变状态，也不报错（同一操作重复提交）
                return state, True
            if state.status.is_terminal():
                if state.status == ReleaseStatus.ROLLED_BACK:
                    raise IllegalTransitionError(
                        "发布已回滚（终态），不能中止；流量已在稳定版本",
                        release_id=release_id,
                    )
                raise IllegalTransitionError(
                    f"状态 {state.status.value} 为终态，不能中止", release_id=release_id
                )
            if not state.status.can_transition_to(ReleaseStatus.ABORTED):
                raise IllegalTransitionError(
                    f"状态 {state.status.value} 不允许中止", release_id=release_id
                )
            # 中止同时把流量收回稳定版本，保证“版本与权重一致且可解释”
            state.current_weight = 0
            state.active_version = state.stable_version
            state.status = ReleaseStatus.ABORTED
            state.last_failure = FailureKind.MANUAL_ABORT
            state.rollback_reason = f"人工中止，流量收回 {state.stable_version}: {reason}"
            state.updated_at = utcnow()
            logger.warning("[abort] release=%s 人工中止，流量收回稳定版本：%s",
                           release_id, reason)
            return state, False

        state, idempotent = self.store.transaction(tx)
        return AdvanceResult(
            release_id=release_id,
            action="aborted" if not idempotent else "already_aborted",
            message="已中止，流量 100% 回到稳定版本" if not idempotent
                    else "发布此前已中止（幂等，状态未改变）",
            state=state, weight_split=self._split(state),
        )

    # ============================================================== 查询

    def get(self, release_id: str) -> ReleaseState:
        def tx(states):
            return self._require(states, release_id)
        return self.store.transaction(tx)

    def list_releases(self) -> list[ReleaseState]:
        return sorted(self.store.load_all().values(), key=lambda s: s.created_at)

    # ============================================================== 内部

    @staticmethod
    def _require(states: dict[str, ReleaseState], release_id: str) -> ReleaseState:
        state = states.get(release_id)
        if state is None:
            raise IllegalTransitionError(f"发布 {release_id!r} 不存在", release_id=release_id)
        return state

    def _apply_rollback(
        self, state: ReleaseState, *, reason: str, failure_kind: FailureKind
    ) -> None:
        """把发布置为一致、可解释的回滚状态：候选 0% / 稳定 100%。"""
        state.current_weight = 0
        state.active_version = state.stable_version
        state.status = ReleaseStatus.ROLLED_BACK
        state.last_failure = failure_kind
        state.rollback_reason = reason
        state.updated_at = utcnow()

    @staticmethod
    def _infer_failure(err: OrchestratorError) -> FailureKind:
        mapping = {
            "probe_timeout": FailureKind.TIMEOUT,
            "consecutive_failures": FailureKind.CONSECUTIVE_FAILURE,
            "manual_abort": FailureKind.MANUAL_ABORT,
        }
        return mapping.get(err.code, FailureKind.CONSECUTIVE_FAILURE)

    @staticmethod
    def _split(state: ReleaseState) -> WeightSplit:
        return WeightSplit(
            candidate_version=state.candidate_version,
            stable_version=state.stable_version,
            candidate_weight=state.current_weight,
            stable_weight=100 - state.current_weight,
        )
