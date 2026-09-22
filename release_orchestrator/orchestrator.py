"""发布编排器核心：推进状态机、健康门禁、回滚与并发安全。

并发安全：
- 进程内为每个 release_id 维护一把 asyncio.Lock；
- advance 采用非阻塞抢锁：同一发布并发推进时，只有一个调用实际执行，
  其余立即得到 advance_conflict 拒绝，保证不重复切换、不丢失状态。

持久化：
- 每次状态变更后立即原子落盘；重启恢复时以批次 completed 标记为准，
  已完成批次绝不重复执行。

异常链路（可程序化区分）：
- ProbeTimeoutError       探测超时 -> 自动回滚
- ConsecutiveFailureError 连续失败达阈值 -> 自动回滚
- ManualAbortError        对已人工中止的发布执行推进/恢复
- IllegalTransitionError  终态/其他非法状态转换
- DuplicateAdvanceError   观察期未满等重复推进
- AdvanceConflictError    并发推进冲突
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from pathlib import Path
from typing import Callable

from .errors import (
    AdvanceConflictError,
    ConsecutiveFailureError,
    DuplicateAdvanceError,
    DuplicateReleaseError,
    IllegalTransitionError,
    ManualAbortError,
    OrchestratorError,
    ProbeTimeoutError,
    ReleaseNotFoundError,
)
from .health import HealthProbeRegistry
from .models import (
    TERMINAL_STATUSES,
    BatchState,
    GateAction,
    HealthVerdict,
    ReleasePlan,
    ReleaseRecord,
    ReleaseStatus,
)
from .storage import JsonFileStore, QuarantinedRecord

logger = logging.getLogger("release_orchestrator.orchestrator")


def validate_plan(plan: ReleasePlan) -> None:
    """计划参数校验（非法输入直接拒绝，区别于运行期状态拒绝）。"""
    if not plan.weights:
        raise ValueError("weights 不能为空")
    if any(not isinstance(w, int) or isinstance(w, bool) for w in plan.weights):
        raise ValueError("weights 必须全部为整数")
    if any(w <= 0 or w > 100 for w in plan.weights):
        raise ValueError("weights 每个值必须在 (0, 100] 区间")
    if any(plan.weights[i] > plan.weights[i + 1] for i in range(len(plan.weights) - 1)):
        raise ValueError("weights 必须单调不减")
    if plan.weights[-1] != 100:
        raise ValueError("weights 最后一批必须为 100（全量）")
    if plan.min_observe_seconds < 0:
        raise ValueError("min_observe_seconds 不能为负")
    if plan.max_consecutive_failures < 1:
        raise ValueError("max_consecutive_failures 必须 >= 1")
    if not plan.candidate or not plan.stable:
        raise ValueError("candidate/stable 不能为空")
    if plan.candidate == plan.stable:
        raise ValueError("candidate 与 stable 不能相同")


class ReleaseOrchestrator:
    def __init__(
        self,
        store: JsonFileStore,
        probes: HealthProbeRegistry,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.store = store
        self.probes = probes
        self._time: Callable[[], float] = clock or time.time
        loaded = store.load()
        self._records: dict[str, ReleaseRecord] = loaded.records
        self._quarantined: list[QuarantinedRecord] = loaded.quarantined
        self._locks: dict[str, asyncio.Lock] = {}
        self._inflight: dict[str, bool] = {}
        self._global_lock = asyncio.Lock()
        self._seq = 0
        for rec in self._records.values():
            self._seq = max(self._seq, len(rec.events))
            self._locks[rec.id] = asyncio.Lock()
            self._inflight[rec.id] = False
        logger.info(
            "orchestrator ready: %d active records, %d quarantined",
            len(self._records), len(self._quarantined),
        )

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        probes: HealthProbeRegistry | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> "ReleaseOrchestrator":
        probes = probes or HealthProbeRegistry()
        return cls(JsonFileStore(path), probes, clock=clock)

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #

    @property
    def quarantined(self) -> list[QuarantinedRecord]:
        return list(self._quarantined)

    def list_releases(self) -> list[ReleaseRecord]:
        return sorted(self._records.values(), key=lambda r: r.created_at)

    def get_release(self, release_id: str) -> ReleaseRecord:
        rec = self._records.get(release_id)
        if rec is None:
            raise ReleaseNotFoundError(f"发布不存在: {release_id}")
        return rec

    def get_events(self, release_id: str) -> list[dict]:
        return list(self.get_release(release_id).events)

    # ------------------------------------------------------------------ #
    # 创建
    # ------------------------------------------------------------------ #

    def create_release(self, plan: ReleasePlan) -> ReleaseRecord:
        validate_plan(plan)
        for existing in self._records.values():
            if existing.plan.name == plan.name:
                raise DuplicateReleaseError(f"同名发布已存在: {plan.name}")
        now = self._time()
        rid = uuid.uuid4().hex[:12]
        record = ReleaseRecord(
            id=rid,
            plan=plan,
            status=ReleaseStatus.PENDING,
            current_batch=0,
            batches=[BatchState(weight=w) for w in plan.weights],
            current_weight=0,
            active_version=plan.stable,
            created_at=now,
            updated_at=now,
        )
        self._append_event(
            record, "release_created",
            {"plan": {
                "name": plan.name, "candidate": plan.candidate, "stable": plan.stable,
                "weights": list(plan.weights), "min_observe_seconds": plan.min_observe_seconds,
                "max_consecutive_failures": plan.max_consecutive_failures,
            }, "decision": "等待首次推进"},
        )
        self._locks[rid] = asyncio.Lock()
        self._inflight[rid] = False
        self._records[rid] = record
        self._persist()
        logger.info(
            "release created id=%s name=%s candidate=%s stable=%s weights=%s",
            rid, plan.name, plan.candidate, plan.stable, plan.weights,
        )
        return record

    # ------------------------------------------------------------------ #
    # 推进（核心状态机）
    # ------------------------------------------------------------------ #

    async def advance(self, release_id: str) -> tuple[ReleaseRecord, GateAction]:
        record = self.get_release(release_id)
        async with self._acquire(release_id, "推进"):
            return await self._advance_locked(record)

    async def _advance_locked(
        self, record: ReleaseRecord
    ) -> tuple[ReleaseRecord, GateAction]:
        status = record.status
        logger.info(
            "advance input: id=%s status=%s current_batch=%d weight=%d active=%s",
            record.id, status.value, record.current_batch,
            record.current_weight, record.active_version,
        )

        if status == ReleaseStatus.ABORTED:
            raise ManualAbortError(f"发布 {record.id} 已被人工中止，拒绝推进")
        if status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
            raise IllegalTransitionError(
                f"发布 {record.id} 处于终态 {status.value}，不允许推进"
            )

        now = self._time()

        # 情况 A：尚未启动 -> 切换到第一批
        if status == ReleaseStatus.PENDING:
            return self._start_first_batch(record, now)

        # 情况 B：暂停中 -> 需要显式 resume
        if status == ReleaseStatus.PAUSED:
            raise DuplicateAdvanceError(
                f"发布 {record.id} 已暂停，请先调用 resume 恢复后再推进"
            )

        # 情况 C：推进中
        batch = record.batches[record.current_batch - 1]
        elapsed = now - (batch.started_at or now)
        if elapsed < record.plan.min_observe_seconds:
            self._append_event(record, "advance_rejected", {
                "reason": "observation_window_not_elapsed",
                "elapsed": round(elapsed, 3),
                "required": record.plan.min_observe_seconds,
                "decision": "保持当前权重，不重复切换",
            })
            self._persist()
            logger.info(
                "advance rejected (waiting): id=%s elapsed=%.3f < required=%.3f -> %s",
                record.id, elapsed, record.plan.min_observe_seconds,
                GateAction.STILL_WAITING.value,
            )
            raise DuplicateAdvanceError(
                f"批次 {record.current_batch} 观察期未满（{elapsed:.3f}s < "
                f"{record.plan.min_observe_seconds}s），本次推进不生效"
            )

        # 观察期已满 -> 过健康门禁
        return self._run_gate(record, batch, now)

    def _start_first_batch(
        self, record: ReleaseRecord, now: float
    ) -> tuple[ReleaseRecord, GateAction]:
        record.status = ReleaseStatus.IN_PROGRESS
        record.current_batch = 1
        batch = record.batches[0]
        batch.started_at = now
        record.current_weight = batch.weight
        record.active_version = record.plan.candidate
        record.updated_at = now
        self._append_event(record, "traffic_switched", {
            "batch": 1, "candidate_weight": batch.weight,
            "stable_weight": 100 - batch.weight,
            "active_version": record.active_version,
            "decision": "发布启动，切到首批金丝雀权重，进入观察期",
        })
        self._persist()
        logger.info(
            "state change: id=%s PENDING -> IN_PROGRESS batch=1 weight=%d",
            record.id, batch.weight,
        )
        return record, GateAction.STARTED

    def _run_gate(
        self, record: ReleaseRecord, batch: BatchState, now: float
    ) -> tuple[ReleaseRecord, GateAction]:
        batch.gate_attempts += 1
        probe = self.probes.get(record.id)
        try:
            result = probe.probe(record.id, record.plan.candidate, batch.weight)
        except ProbeTimeoutError as exc:
            # 探测超时：独立异常链路 + 自动回滚
            self._append_event(record, "gate_probe_timeout", {
                "batch": record.current_batch, "attempt": batch.gate_attempts,
                "decision": "探测超时 -> 立即回滚到稳定版本",
            })
            self._rollback(record, f"健康探测超时（第 {batch.gate_attempts} 次尝试）", now)
            logger.error("gate PROBE TIMEOUT id=%s -> rollback", record.id)
            exc.record = record  # type: ignore[attr-defined]
            raise

        verdict = result.verdict
        if verdict == HealthVerdict.DEGRADED:
            record.status = ReleaseStatus.PAUSED
            record.updated_at = now
            self._append_event(record, "gate_paused", {
                "batch": record.current_batch, "attempt": batch.gate_attempts,
                "verdict": verdict.value, "reason": result.reason,
                "metrics": result.metrics,
                "candidate_weight": record.current_weight,
                "decision": "指标降级 -> 暂停推进，保持当前权重，等待人工 resume",
            })
            self._persist()
            logger.warning(
                "gate DEGRADED id=%s batch=%d reason=%s -> PAUSED (weight kept %d)",
                record.id, record.current_batch, result.reason, record.current_weight,
            )
            return record, GateAction.PAUSED

        if verdict == HealthVerdict.UNHEALTHY:
            batch.consecutive_failures += 1
            threshold = record.plan.max_consecutive_failures
            self._append_event(record, "gate_unhealthy", {
                "batch": record.current_batch, "attempt": batch.gate_attempts,
                "consecutive_failures": batch.consecutive_failures,
                "threshold": threshold, "reason": result.reason, "metrics": result.metrics,
            })
            if batch.consecutive_failures >= threshold:
                self._append_event(record, "gate_threshold_exceeded", {
                    "batch": record.current_batch,
                    "consecutive_failures": batch.consecutive_failures,
                    "decision": f"连续失败达到阈值 {threshold} -> 立即回滚",
                })
                self._rollback(
                    record,
                    f"健康连续失败 {batch.consecutive_failures} 次达到阈值 {threshold}"
                    + (f"（{result.reason}）" if result.reason else ""),
                    now,
                )
                err = ConsecutiveFailureError(
                    f"发布 {record.id} 连续健康失败 {batch.consecutive_failures} 次，"
                    f"达到阈值 {threshold}，已回滚"
                )
                err.record = record  # type: ignore[attr-defined]
                logger.error(
                    "gate CONSECUTIVE FAILURE id=%s failures=%d -> rollback",
                    record.id, batch.consecutive_failures,
                )
                raise err
            record.updated_at = now
            self._persist()
            logger.warning(
                "gate UNHEALTHY id=%s failures=%d/%d -> 保持权重等待再次观察",
                record.id, batch.consecutive_failures, threshold,
            )
            # 未达阈值：本次门禁未通过，权重不变，调用方得到可区分的等待结果
            raise DuplicateAdvanceError(
                f"批次 {record.current_batch} 健康检查未通过（连续失败 "
                f"{batch.consecutive_failures}/{threshold}），保持当前权重"
            )

        # verdict == HEALTHY：完成当前批次
        batch.consecutive_failures = 0
        batch.completed = True
        batch.completed_at = now
        record.completed_batches += 1
        record.updated_at = now

        if record.current_batch == len(record.batches):
            # 末批通过 -> 全量完成
            record.status = ReleaseStatus.COMPLETED
            record.current_weight = 100
            record.active_version = record.plan.candidate
            self._append_event(record, "release_completed", {
                "batch": record.current_batch,
                "candidate_weight": 100, "active_version": record.active_version,
                "decision": "全部批次门禁通过，候选版本全量，发布完成",
            })
            self._persist()
            logger.info(
                "state change: id=%s -> COMPLETED active=%s weight=100",
                record.id, record.active_version,
            )
            return record, GateAction.COMPLETED

        # 推进到下一批
        next_index = record.current_batch + 1
        next_batch = record.batches[next_index - 1]
        next_batch.started_at = now
        record.current_batch = next_index
        record.current_weight = next_batch.weight
        record.updated_at = now
        self._append_event(record, "batch_completed", {
            "completed_batch": record.current_batch - 1,
            "decision": "门禁通过且观察期满足",
            "reason": result.reason, "metrics": result.metrics,
        })
        self._append_event(record, "traffic_switched", {
            "batch": next_index, "candidate_weight": next_batch.weight,
            "stable_weight": 100 - next_batch.weight,
            "active_version": record.active_version,
            "decision": f"推进到第 {next_index} 批金丝雀权重，重新进入观察期",
        })
        self._persist()
        logger.info(
            "state change: id=%s gate HEALTHY -> batch=%d weight=%d",
            record.id, next_index, next_batch.weight,
        )
        return record, GateAction.ADVANCED

    # ------------------------------------------------------------------ #
    # 回滚 / 中止 / 恢复
    # ------------------------------------------------------------------ #

    def _rollback(self, record: ReleaseRecord, reason: str, now: float) -> None:
        record.status = ReleaseStatus.ROLLED_BACK
        record.current_weight = 0
        record.active_version = record.plan.stable
        record.rollback_reason = reason
        record.updated_at = now
        self._append_event(record, "rollback", {
            "reason": reason,
            "candidate_weight": 0, "stable_weight": 100,
            "active_version": record.plan.stable,
            "decision": "流量全部回到稳定版本，版本与权重恢复一致",
        })
        self._persist()
        logger.error(
            "state change: id=%s -> ROLLED_BACK active=%s weight=0 reason=%s",
            record.id, record.plan.stable, reason,
        )

    async def abort(self, release_id: str, reason: str = "") -> ReleaseRecord:
        record = self.get_release(release_id)
        async with self._acquire(release_id, "中止"):
            now = self._time()
            if record.status == ReleaseStatus.ABORTED:
                raise DuplicateAdvanceError(f"发布 {release_id} 已处于中止状态")
            if record.status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
                raise IllegalTransitionError(
                    f"发布 {release_id} 处于终态 {record.status.value}，不允许中止"
                )
            record.status = ReleaseStatus.ABORTED
            record.rollback_reason = reason or None
            record.updated_at = now
            self._append_event(record, "manual_abort", {
                "reason": reason, "candidate_weight": record.current_weight,
                "active_version": record.active_version,
                "decision": "人工中止：冻结当前流量，不再推进/自动回滚",
            })
            self._persist()
            logger.warning(
                "state change: id=%s -> ABORTED weight=%d active=%s reason=%s",
                record.id, record.current_weight, record.active_version, reason,
            )
            return record

    async def resume(self, release_id: str) -> ReleaseRecord:
        record = self.get_release(release_id)
        async with self._acquire(release_id, "恢复"):
            now = self._time()
            if record.status == ReleaseStatus.ABORTED:
                raise ManualAbortError(
                    f"发布 {release_id} 已被人工中止，不可恢复（请新建发布）"
                )
            if record.status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
                raise IllegalTransitionError(
                    f"发布 {release_id} 处于终态 {record.status.value}，不允许恢复"
                )
            if record.status != ReleaseStatus.PAUSED:
                raise IllegalTransitionError(
                    f"发布 {release_id} 当前为 {record.status.value}，仅暂停态可恢复"
                )
            record.status = ReleaseStatus.IN_PROGRESS
            batch = record.batches[record.current_batch - 1]
            batch.started_at = now  # 恢复后重新计算观察期
            record.updated_at = now
            self._append_event(record, "resumed", {
                "batch": record.current_batch, "candidate_weight": record.current_weight,
                "decision": "人工恢复：观察期重新计时，下一次 advance 将重新过门禁",
            })
            self._persist()
            logger.info(
                "state change: id=%s PAUSED -> IN_PROGRESS batch=%d weight=%d",
                record.id, record.current_batch, record.current_weight,
            )
            return record

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #

    @contextlib.asynccontextmanager
    async def _acquire(self, release_id: str, op_name: str):
        """获取某发布的执行权。

        已存在在途操作（无论它此刻是否正好持有锁、是否在 await）时，
        立即返回 advance_conflict，保证并发推进只有一个生效结果。
        """
        lock = self._locks[release_id]
        if self._inflight.get(release_id) or lock.locked():
            logger.warning(
                "conflict rejected: id=%s op=%s (inflight=%s locked=%s)",
                release_id, op_name, self._inflight.get(release_id), lock.locked(),
            )
            raise AdvanceConflictError(
                f"发布 {release_id} 已有在途操作（{op_name}），本次调用未生效"
            )
        self._inflight[release_id] = True
        try:
            async with lock:
                # 协作式调度边界：真实系统此处会有文件/网络 I/O 挂起；
                # 显式让出一次，使并发任务能观察到在途标志并被识别为冲突，
                # 而不是在临界区结束后串行进入并误判为"观察期未满"。
                await asyncio.sleep(0)
                yield
        finally:
            self._inflight[release_id] = False

    def _append_event(self, record: ReleaseRecord, event_type: str, detail: dict) -> None:
        event = {
            "seq": len(record.events) + 1,
            "timestamp": self._time(),
            "type": event_type,
            "detail": detail,
        }
        record.events.append(event)
        logger.info("event id=%s seq=%d type=%s detail=%s",
                    record.id, event["seq"], event_type, detail)

    def _persist(self) -> None:
        self.store.save_all(self._records, self._quarantined)
