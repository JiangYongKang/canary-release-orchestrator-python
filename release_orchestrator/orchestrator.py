"""发布编排器核心：推进状态机、健康门禁、回滚与多实例并发安全。

多实例并发安全（多个服务进程共用同一份 JSON 状态）：
- 进程内为每个 release_id 维护一把 asyncio.Lock + 在途标志，同进程并发调用
  立即得到 advance_conflict，不必争抢文件锁；
- 跨进程为每个 release_id 持有一把非阻塞文件锁（storage.try_acquire_release_lock）：
  同一发布被多个进程同时推进/恢复/中止时，只有一个实例能拿到操作权真正执行，
  其余立即以 advance_conflict 返回"未生效"。锁按发布隔离，不同发布互不排队；
- 拿到操作权后一律从磁盘**重新读取最新状态**，在深拷贝的工作副本上做变更，
  探针等耗时动作不持提交锁；提交时用记录版本号做乐观校验（commit_record）：
  若期间该发布已被别的实例改过（陈旧实例长睡/暂停后继续/重启回来），
  本次写入被拒绝（stale_state），并把拒绝原因追加进最新状态的事件链。
  绝不状态回退、不重复完成批次、不把终态写回进行中；
- 查询（get/list/events/health）每次读取磁盘最新快照，不使用长期内存缓存，
  因此任何实例都能立刻看到其他实例的推进结果；
- 每次提交只合并本发布记录，其他发布原样保留，顶层 revision +1。

持久化：
- 存储原子落盘（临时文件 + os.replace）；重启恢复时以批次 completed 标记为准，
  已完成批次绝不重复执行；旧版无版本号数据按 version=0 加载，首次提交自动升级。

异常链路（可程序化区分）：
- ProbeTimeoutError       探测超时 -> 自动回滚
- ConsecutiveFailureError 连续失败达阈值 -> 自动回滚
- ManualAbortError        对已人工中止的发布执行推进/恢复
- IllegalTransitionError  终态/其他非法状态转换
- DuplicateAdvanceError   观察期未满等重复推进
- AdvanceConflictError    并发冲突（同进程在途 / 其他进程持有该发布操作权）
- StaleStateError         基于过期进度的变更被最新共享状态拒绝
"""

from __future__ import annotations

import asyncio
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
    ProbeTimeoutError,
    ReleaseNotFoundError,
    StaleStateError,
    StorageBusyError,
)
from .health import HealthProbeRegistry
from .models import (
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
        # 进程内每发布串行/在途标志（快速路径，避免同进程并发去抢文件锁）
        self._locks: dict[str, asyncio.Lock] = {}
        self._inflight: dict[str, bool] = {}
        snap = store.read_snapshot()
        for rid in snap.records:
            self._locks[rid] = asyncio.Lock()
            self._inflight[rid] = False
        # 启动加载时发现的隔离标记立即持久化（保持"明确标记、不静默吞掉"语义）
        if snap.quarantined:
            try:
                store.commit_quarantined(snap.quarantined)
            except Exception:  # pragma: no cover - 持久化失败不应阻止启动
                logger.exception("persist quarantine markers on startup failed")
        logger.info(
            "orchestrator ready: %d active records, %d quarantined, revision=%d",
            len(snap.records), len(snap.quarantined), snap.revision,
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
    # 查询（每次都读共享状态的最新快照，不使用长期内存缓存）
    # ------------------------------------------------------------------ #

    @property
    def quarantined(self) -> list[QuarantinedRecord]:
        return self.store.read_snapshot().quarantined

    def list_releases(self) -> list[ReleaseRecord]:
        return sorted(self.store.read_snapshot().records.values(),
                      key=lambda r: r.created_at)

    def get_release(self, release_id: str) -> ReleaseRecord:
        rec, _ = self._load_latest(release_id)
        return rec

    def get_events(self, release_id: str) -> list[dict]:
        return list(self.get_release(release_id).events)

    def _load_latest(self, release_id: str) -> tuple[ReleaseRecord, int]:
        """读取磁盘最新记录（深拷贝）与其乐观版本号。"""
        try:
            return self.store.get_record(release_id)
        except KeyError:
            raise ReleaseNotFoundError(f"发布不存在: {release_id}") from None

    # ------------------------------------------------------------------ #
    # 创建
    # ------------------------------------------------------------------ #

    def create_release(self, plan: ReleasePlan) -> ReleaseRecord:
        validate_plan(plan)
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
        # 创建走全局提交锁串行化：在锁内复查同名冲突，避免两个进程同时建同名发布
        try:
            with self.store.commit_lock():
                snap = self.store.read_snapshot()
                for existing in snap.records.values():
                    if existing.plan.name == plan.name:
                        raise DuplicateReleaseError(f"同名发布已存在: {plan.name}")
                self.store.commit_record(record, expected_version=0)
        except StorageBusyError:
            raise
        self._ensure_local_lock(rid)
        logger.info(
            "release created id=%s name=%s candidate=%s stable=%s weights=%s",
            rid, plan.name, plan.candidate, plan.stable, plan.weights,
        )
        return self.get_release(rid)

    # ------------------------------------------------------------------ #
    # 推进 / 恢复 / 中止（多实例协作的统一变更管线）
    # ------------------------------------------------------------------ #

    async def advance(self, release_id: str) -> tuple[ReleaseRecord, GateAction]:
        return await self._mutate(
            release_id, "推进", lambda rec, ver: self._advance_locked(rec, ver)
        )

    async def resume(self, release_id: str) -> ReleaseRecord:
        result = await self._mutate(
            release_id, "恢复", lambda rec, ver: (self._resume_locked(rec, ver), None)
        )
        return result[0]

    async def abort(self, release_id: str, reason: str = "") -> ReleaseRecord:
        result = await self._mutate(
            release_id, "中止",
            lambda rec, ver: (self._abort_locked(rec, ver, reason), None),
        )
        return result[0]

    async def _mutate(self, release_id: str, op_name: str, fn) -> tuple:
        """统一变更管线：进程内在途检查 -> 跨发布文件锁 -> 读最新 -> 变更 -> 乐观提交。"""
        # 存在性先查一次（拿不到记录直接 404，不产生锁文件）
        self.get_release(release_id)

        local = self._ensure_local_lock(release_id)
        if self._inflight.get(release_id) or local.locked():
            logger.warning(
                "conflict rejected (in-process): id=%s op=%s inflight=%s",
                release_id, op_name, self._inflight.get(release_id),
            )
            raise AdvanceConflictError(
                f"发布 {release_id} 已有在途操作（{op_name}），本次调用未生效"
            )
        self._inflight[release_id] = True
        file_lock = None
        try:
            # 协作式让出：让同批并发任务先观察到在途标志（保持单事件循环单赢家语义）
            await asyncio.sleep(0)
            file_lock = self.store.try_acquire_release_lock(release_id)
            if file_lock is None:
                logger.warning(
                    "conflict rejected (other instance holds op lock): id=%s op=%s",
                    release_id, op_name,
                )
                raise AdvanceConflictError(
                    f"发布 {release_id} 正被其他服务实例操作（{op_name}），本次调用未生效"
                )

            # 关键：以共享状态里的最新进度作为变更基线，不使用任何缓存副本
            record, version = self._load_latest(release_id)
            logger.info(
                "op starts: id=%s op=%s base_version=%d status=%s batch=%d weight=%d",
                release_id, op_name, version, record.status.value,
                record.current_batch, record.current_weight,
            )
            try:
                outcome = fn(record, version)
            except _CommitAndReraise as signal:
                # 变更已产生（如回滚），先落盘再把业务异常抛出
                self._commit_working_copy(signal.record, signal.version)
                signal.error.record = signal.record
                raise signal.error
            record, _aux = outcome
            self._commit_working_copy(record, version)
            return outcome
        finally:
            if file_lock is not None:
                file_lock.release()
            self._inflight[release_id] = False

    def _commit_working_copy(self, record: ReleaseRecord, expected_version: int) -> None:
        """提交工作副本；版本过期时把拒绝原因记录进最新状态后抛 stale_state。"""
        try:
            self.store.commit_record(record, expected_version)
        except StaleStateError as exc:
            self._record_stale_rejection(record, expected_version)
            exc.release_id = record.id  # type: ignore[attr-defined]
            exc.record = self.get_release(record.id)  # type: ignore[attr-defined]
            raise

    def _record_stale_rejection(
        self, stale_record: ReleaseRecord, expected_version: int
    ) -> None:
        """陈旧操作被拒绝：以最新状态为准追加 operation_rejected 事件并提交。

        调用方（_mutate）仍持有该发布的跨进程操作锁，故此处直接读-改-提交，
        不再重复抢锁（同进程用第二个 fd 抢同一 flock 会被自身拒绝）。
        终态记录只追加事件，不改变状态/进度，因此不存在"把终态写回进行中"。
        """
        try:
            latest, latest_version = self._load_latest(stale_record.id)
            if latest.status == stale_record.status and latest.current_batch == stale_record.current_batch:
                # 状态并未变化（极小概率的版本竞争）：无需追加
                return
            self._append_event(latest, "operation_rejected", {
                "reason": "stale_state",
                "rejected_op_status": stale_record.status.value,
                "rejected_op_batch": stale_record.current_batch,
                "rejected_op_weight": stale_record.current_weight,
                "base_version": expected_version,
                "latest_status": latest.status.value,
                "latest_batch": latest.current_batch,
                "decision": (
                    "本次操作基于过期进度，共享状态已被其他实例更新；"
                    "以共享状态最新进度为准，本次未生效"
                ),
            })
            self.store.commit_record(latest, latest_version)
            logger.warning(
                "stale rejection recorded: id=%s wanted=%s@b%s latest=%s@b%s",
                latest.id, stale_record.status.value, stale_record.current_batch,
                latest.status.value, latest.current_batch,
            )
        except (StaleStateError, ReleaseNotFoundError):  # 极端竞争：不掩盖原始拒绝
            logger.warning(
                "stale rejection event raced again: id=%s", stale_record.id,
            )

    # ------------------------------------------------------------------ #
    # 推进状态机（在工作副本上执行，不直接落盘；提交统一由 _mutate 完成）
    # ------------------------------------------------------------------ #

    def _advance_locked(
        self, record: ReleaseRecord, version: int
    ) -> tuple[ReleaseRecord, GateAction]:
        status = record.status
        logger.info(
            "advance input: id=%s status=%s current_batch=%d weight=%d active=%s",
            record.id, status.value, record.current_batch,
            record.current_weight, record.active_version,
        )

        if status == ReleaseStatus.ABORTED:
            self._reject_event(
                record, "advance", "manual_aborted",
                "发布已被人工中止，拒绝推进",
            )
            raise _CommitAndReraise(
                record, version,
                ManualAbortError(f"发布 {record.id} 已被人工中止，拒绝推进"),
            )
        if status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
            self._reject_event(
                record, "advance", f"terminal_{status.value}",
                f"发布处于终态 {status.value}，不允许推进",
            )
            raise _CommitAndReraise(
                record, version,
                IllegalTransitionError(
                    f"发布 {record.id} 处于终态 {status.value}，不允许推进"
                ),
            )

        now = self._time()

        # 情况 A：尚未启动 -> 切换到第一批
        if status == ReleaseStatus.PENDING:
            return self._start_first_batch(record, now)

        # 情况 B：暂停中 -> 需要显式 resume
        if status == ReleaseStatus.PAUSED:
            self._reject_event(
                record, "advance", "paused_requires_resume",
                "已暂停，请先调用 resume 恢复后再推进",
            )
            raise _CommitAndReraise(
                record, version,
                DuplicateAdvanceError(
                    f"发布 {record.id} 已暂停，请先调用 resume 恢复后再推进"
                ),
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
            logger.info(
                "advance rejected (waiting): id=%s elapsed=%.3f < required=%.3f -> %s",
                record.id, elapsed, record.plan.min_observe_seconds,
                GateAction.STILL_WAITING.value,
            )
            raise _CommitAndReraise(
                record, version,
                DuplicateAdvanceError(
                    f"批次 {record.current_batch} 观察期未满（{elapsed:.3f}s < "
                    f"{record.plan.min_observe_seconds}s），本次推进不生效"
                ),
            )

        # 观察期已满 -> 过健康门禁
        return self._run_gate(record, batch, now, version)

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
        logger.info(
            "state change: id=%s PENDING -> IN_PROGRESS batch=1 weight=%d",
            record.id, batch.weight,
        )
        return record, GateAction.STARTED

    def _run_gate(
        self, record: ReleaseRecord, batch: BatchState, now: float, version: int
    ) -> tuple[ReleaseRecord, GateAction]:
        batch.gate_attempts += 1
        probe = self.probes.get(record.id)
        try:
            result = probe.probe(record.id, record.plan.candidate, batch.weight)
        except ProbeTimeoutError as exc:
            # 探测超时：独立异常链路 + 自动回滚（工作副本先变更，随异常一起提交）
            self._append_event(record, "gate_probe_timeout", {
                "batch": record.current_batch, "attempt": batch.gate_attempts,
                "decision": "探测超时 -> 立即回滚到稳定版本",
            })
            self._rollback(record, f"健康探测超时（第 {batch.gate_attempts} 次尝试）", now)
            logger.error("gate PROBE TIMEOUT id=%s -> rollback", record.id)
            raise _CommitAndReraise(record, version, exc)

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
                logger.error(
                    "gate CONSECUTIVE FAILURE id=%s failures=%d -> rollback",
                    record.id, batch.consecutive_failures,
                )
                raise _CommitAndReraise(record, version, err)
            record.updated_at = now
            logger.warning(
                "gate UNHEALTHY id=%s failures=%d/%d -> 保持权重等待再次观察",
                record.id, batch.consecutive_failures, threshold,
            )
            # 未达阈值：本次门禁未通过，但失败计数 +1 与事件已产生，需落盘
            raise _CommitAndReraise(
                record, version,
                DuplicateAdvanceError(
                    f"批次 {record.current_batch} 健康检查未通过（连续失败 "
                    f"{batch.consecutive_failures}/{threshold}），保持当前权重"
                ),
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
        logger.info(
            "state change: id=%s gate HEALTHY -> batch=%d weight=%d",
            record.id, next_index, next_batch.weight,
        )
        return record, GateAction.ADVANCED

    # ------------------------------------------------------------------ #
    # 回滚 / 中止 / 恢复（工作副本变更）
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
        logger.error(
            "state change: id=%s -> ROLLED_BACK active=%s weight=0 reason=%s",
            record.id, record.plan.stable, reason,
        )

    def _abort_locked(
        self, record: ReleaseRecord, version: int, reason: str
    ) -> ReleaseRecord:
        now = self._time()
        if record.status == ReleaseStatus.ABORTED:
            self._reject_event(record, "abort", "already_aborted", "发布已处于中止状态")
            raise _CommitAndReraise(
                record, version,
                DuplicateAdvanceError(f"发布 {record.id} 已处于中止状态"),
            )
        if record.status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
            self._reject_event(
                record, "abort", f"terminal_{record.status.value}",
                f"发布处于终态 {record.status.value}，不允许中止",
            )
            raise _CommitAndReraise(
                record, version,
                IllegalTransitionError(
                    f"发布 {record.id} 处于终态 {record.status.value}，不允许中止"
                ),
            )
        record.status = ReleaseStatus.ABORTED
        record.rollback_reason = reason or None
        record.updated_at = now
        self._append_event(record, "manual_abort", {
            "reason": reason, "candidate_weight": record.current_weight,
            "active_version": record.active_version,
            "decision": "人工中止：冻结当前流量，不再推进/自动回滚",
        })
        logger.warning(
            "state change: id=%s -> ABORTED weight=%d active=%s reason=%s",
            record.id, record.current_weight, record.active_version, reason,
        )
        return record

    def _resume_locked(self, record: ReleaseRecord, version: int) -> ReleaseRecord:
        now = self._time()
        if record.status == ReleaseStatus.ABORTED:
            self._reject_event(
                record, "resume", "manual_aborted",
                "发布已被人工中止，不可恢复",
            )
            raise _CommitAndReraise(
                record, version,
                ManualAbortError(
                    f"发布 {record.id} 已被人工中止，不可恢复（请新建发布）"
                ),
            )
        if record.status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
            self._reject_event(
                record, "resume", f"terminal_{record.status.value}",
                f"发布处于终态 {record.status.value}，不允许恢复",
            )
            raise _CommitAndReraise(
                record, version,
                IllegalTransitionError(
                    f"发布 {record.id} 处于终态 {record.status.value}，不允许恢复"
                ),
            )
        if record.status != ReleaseStatus.PAUSED:
            self._reject_event(
                record, "resume", f"not_paused_{record.status.value}",
                f"当前为 {record.status.value}，仅暂停态可恢复",
            )
            raise _CommitAndReraise(
                record, version,
                IllegalTransitionError(
                    f"发布 {record.id} 当前为 {record.status.value}，仅暂停态可恢复"
                ),
            )
        record.status = ReleaseStatus.IN_PROGRESS
        batch = record.batches[record.current_batch - 1]
        batch.started_at = now  # 恢复后重新计算观察期
        record.updated_at = now
        self._append_event(record, "resumed", {
            "batch": record.current_batch, "candidate_weight": record.current_weight,
            "decision": "人工恢复：观察期重新计时，下一次 advance 将重新过门禁",
        })
        logger.info(
            "state change: id=%s PAUSED -> IN_PROGRESS batch=%d weight=%d",
            record.id, record.current_batch, record.current_weight,
        )
        return record

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #

    def _ensure_local_lock(self, release_id: str) -> asyncio.Lock:
        lock = self._locks.get(release_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[release_id] = lock
            self._inflight.setdefault(release_id, False)
        return lock

    def _reject_event(
        self, record: ReleaseRecord, op: str, reason: str, decision: str
    ) -> None:
        """把"未生效/被拒绝"原因写进事件链（查询结果与事件记录都可追溯）。"""
        self._append_event(record, "operation_rejected", {
            "op": op,
            "reason": reason,
            "status": record.status.value,
            "current_batch": record.current_batch,
            "current_weight": record.current_weight,
            "decision": f"{decision}；本次未生效",
        })

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


class _CommitAndReraise(Exception):
    """内部信号：工作副本已变更（回滚），先提交再把业务异常抛给调用方。"""

    def __init__(self, record: ReleaseRecord, version: int, error: Exception) -> None:
        self.record = record
        self.version = version
        self.error = error
