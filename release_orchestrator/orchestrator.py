"""发布编排器核心：推进状态机、健康门禁、回滚与多实例并发协作。

多实例（多个本地服务进程 / 多线程共享同一份状态文件）协作规则：

- 每个发布一把跨进程文件锁（``<状态文件目录>/.locks/release-<id>.lock``，
  基于 fcntl.flock，见 locking.py）：同一发布的 advance / resume / abort
  跨进程、跨线程互斥；抢锁为**非阻塞**，抢不到立即返回 ``advance_conflict``，
  本次调用不生效——同一次流量切换不会执行两遍。
- 另有一把全局状态锁（``global.state.lock``），只在毫秒级的
  "读盘上最新快照 -> 变更 -> 原子落盘"提交阶段短暂持有；不同发布各自有
  独立的发布锁，互不排队。
- 任何变更都在全局锁内以**共享状态里的最新记录**为基准，实例内存中的
  ReleaseRecord 仅作查询缓存：运行很久 / 暂停后继续 / 重启回来的实例都
  不可能用旧进度覆盖他人的新结果，终态（completed/rolled_back/aborted）
  也不可能被写回 in_progress。
- 每次生效的变更使记录 ``version`` 单调 +1。调用方可传 ``expected_version``
  做乐观并发校验：与共享状态不一致时返回 ``stale_version``，调用未生效，
  并留下 ``operation_rejected`` 事件，查询结果与事件链都能看到拒绝原因。
- 持锁进程异常退出时锁由内核自动释放；状态文件仍是临时文件 + os.replace
  原子替换，读端永远读到完整文件。

异常链路（可程序化区分）：
- ProbeTimeoutError        探测超时 -> 自动回滚
- ConsecutiveFailureError  连续失败达阈值 -> 自动回滚
- ManualAbortError         对已人工中止的发布执行推进/恢复
- IllegalTransitionError   终态/其他非法状态转换
- DuplicateAdvanceError    观察期未满等重复推进
- AdvanceConflictError     并发冲突（发布锁被占，本次未生效）
- StaleVersionError        调用方持有陈旧版本（本次未生效）
- StateCommitBusyError     状态提交锁等待超时（本次未生效，可重试）
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .errors import (
    AdvanceConflictError,
    ConsecutiveFailureError,
    DuplicateAdvanceError,
    DuplicateReleaseError,
    IllegalTransitionError,
    IncompatibleStateError,
    ManualAbortError,
    ProbeTimeoutError,
    ReleaseNotFoundError,
    StaleVersionError,
    StateCommitBusyError,
)
from .health import HealthProbeRegistry
from .locking import FileLock, LockBusy, LockManager
from .models import (
    BatchState,
    GateAction,
    HealthVerdict,
    ReleasePlan,
    ReleaseRecord,
    ReleaseStatus,
)
from .storage import JsonFileStore, LoadResult, QuarantinedRecord

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


@dataclass
class _Txn:
    """一次状态提交的事务上下文：以盘上最新快照为底，结束时一次性落盘。"""

    records: dict[str, ReleaseRecord]
    quarantined: list[QuarantinedRecord]
    dirty: bool = False

    def persist(self) -> None:
        """状态机在实际产生变更后调用；提交器据此在结束时落盘一次。"""
        self.dirty = True


class ReleaseOrchestrator:
    def __init__(
        self,
        store: JsonFileStore,
        probes: HealthProbeRegistry,
        *,
        clock: Callable[[], float] | None = None,
        commit_timeout: float = 5.0,
        instance_id: str | None = None,
    ) -> None:
        self.store = store
        self.probes = probes
        self._time: Callable[[], float] = clock or time.time
        self._commit_timeout = max(0.05, commit_timeout)
        self._instance_id = instance_id or uuid.uuid4().hex[:8]
        self._locks = LockManager(store.path)
        loaded = store.load()
        self._records: dict[str, ReleaseRecord] = loaded.records
        self._quarantined: list[QuarantinedRecord] = loaded.quarantined
        self._cache_lock = threading.Lock()
        # 进程内每发布协程锁：同一事件循环内 gather 的并发调用，只有第一个
        # 能非阻塞拿到；其余立即 advance_conflict（保持既有单赢家语义）。
        # 真正跨进程/跨线程的互斥由 flock 发布锁兜底。
        self._async_locks: dict[str, asyncio.Lock] = {
            rid: asyncio.Lock() for rid in self._records
        }
        logger.info(
            "orchestrator ready: instance=%s %d records, %d quarantined, state=%s",
            self._instance_id, len(self._records), len(self._quarantined), store.path,
        )

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        probes: HealthProbeRegistry | None = None,
        *,
        clock: Callable[[], float] | None = None,
        commit_timeout: float = 5.0,
        instance_id: str | None = None,
    ) -> "ReleaseOrchestrator":
        probes = probes or HealthProbeRegistry()
        return cls(
            JsonFileStore(path), probes,
            clock=clock, commit_timeout=commit_timeout, instance_id=instance_id,
        )

    # ------------------------------------------------------------------ #
    # 查询：总是以共享状态最新快照为准（内存记录仅为缓存）
    # ------------------------------------------------------------------ #

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def quarantined(self) -> list[QuarantinedRecord]:
        self._refresh_cache()
        with self._cache_lock:
            return list(self._quarantined)

    def list_releases(self) -> list[ReleaseRecord]:
        self._refresh_cache()
        with self._cache_lock:
            return sorted(self._records.values(), key=lambda r: (r.created_at, r.id))

    def get_release(self, release_id: str) -> ReleaseRecord:
        self._refresh_cache()
        with self._cache_lock:
            rec = self._records.get(release_id)
            if rec is None:
                raise ReleaseNotFoundError(f"发布不存在: {release_id}")
            return rec

    def get_events(self, release_id: str) -> list[dict]:
        return list(self.get_release(release_id).events)

    def _refresh_cache(self) -> None:
        """从盘上重读最新快照刷新内存缓存。

        - 文件不存在且内存中有启动期整体隔离记录（坏文件已被改名移走）时，
          保留该隔离结论，不用空快照把它冲掉；
        - 读取失败（IO/损坏/版本不符）时保留最后已知缓存，查询不因并发写入抖动失败。
        """
        if not self.store.path.exists():
            with self._cache_lock:
                whole_file_quarantined = any(
                    q.release_id == "<whole-state-file>" for q in self._quarantined
                )
            if whole_file_quarantined:
                return
        try:
            snap = self.store.read_snapshot()
        except (OSError, ValueError, IncompatibleStateError) as exc:
            logger.warning(
                "refresh shared state failed, serving last-known cache: %s", exc
            )
            return
        self._install_snapshot(snap)

    def _install_snapshot(self, snap: LoadResult) -> None:
        with self._cache_lock:
            self._records = dict(snap.records)
            self._quarantined = list(snap.quarantined)

    # ------------------------------------------------------------------ #
    # 创建（全局锁内查重，无需发布锁）
    # ------------------------------------------------------------------ #

    def create_release(self, plan: ReleasePlan) -> ReleaseRecord:
        validate_plan(plan)
        glock = self._locks.global_state()
        try:
            with glock.hold(wait_timeout=self._commit_timeout):
                return self._create_locked(plan)
        except LockBusy as exc:
            raise StateCommitBusyError(
                f"共享状态提交锁被占用（{self._commit_timeout}s 内未获取），创建未生效"
            ) from exc

    def _create_locked(self, plan: ReleasePlan) -> ReleaseRecord:
        snap = self._read_latest()
        records = dict(snap.records)
        quarantined = list(snap.quarantined)
        for existing in records.values():
            if existing.plan.name == plan.name:
                self._install_snapshot(snap)
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
        records[rid] = record
        create_txn = _Txn(records=records, quarantined=quarantined, dirty=True)
        self._write_snapshot(create_txn, record)
        logger.info(
            "release created instance=%s id=%s name=%s candidate=%s stable=%s weights=%s",
            self._instance_id, rid, plan.name, plan.candidate, plan.stable, plan.weights,
        )
        return record

    # ------------------------------------------------------------------ #
    # 变更操作的统一并发包装
    # ------------------------------------------------------------------ #

    async def _with_exclusive(
        self,
        release_id: str,
        op_name: str,
        mutator: Callable[[ReleaseRecord, _Txn], object],
        expected_version: int | None = None,
    ) -> object:
        # 第一步：非阻塞抢进程内 asyncio 锁（同一事件循环并发调用单赢家）。
        alock = self._async_locks.setdefault(release_id, asyncio.Lock())
        if alock.locked():
            logger.warning(
                "conflict rejected: instance=%s id=%s op=%s (in-loop op in flight)",
                self._instance_id, release_id, op_name,
            )
            raise AdvanceConflictError(
                f"发布 {release_id} 已有在途操作（{op_name}），本次调用未生效"
            )
        await alock.acquire()
        # 协作式调度边界：让同循环内 gather 的其他调用观察到锁已被占，
        # 而不是串行进入临界区后误判为"观察期未满"。
        await asyncio.sleep(0)
        # 第二步：非阻塞抢该发布的跨进程 flock——跨进程/跨线程单赢家。
        rlock = self._locks.for_release(release_id)
        rfd: int | None = None
        try:
            try:
                rfd = rlock.acquire_nb()
            except LockBusy:
                logger.warning(
                    "conflict rejected: instance=%s id=%s op=%s "
                    "(flock held by another instance)",
                    self._instance_id, release_id, op_name,
                )
                raise AdvanceConflictError(
                    f"发布 {release_id} 已有在途操作（{op_name}），本次调用未生效"
                )
            # 第三步：限界等待全局提交锁（不同发布仅在毫秒级提交阶段竞争）。
            gfd = await self._acquire_global_async(op_name, release_id)
            try:
                return self._commit(release_id, op_name, mutator, expected_version)
            finally:
                self._locks.global_state().release_fd(gfd)
        finally:
            if rfd is not None:
                rlock.release_fd(rfd)
            alock.release()

    async def _acquire_global_async(self, op_name: str, release_id: str) -> int:
        lock = self._locks.global_state()
        deadline = time.monotonic() + self._commit_timeout
        attempt = 0
        while True:
            try:
                return lock.acquire_nb()
            except LockBusy:
                if time.monotonic() >= deadline:
                    logger.warning(
                        "commit lock timeout: instance=%s id=%s op=%s after %.2fs",
                        self._instance_id, release_id, op_name, self._commit_timeout,
                    )
                    raise StateCommitBusyError(
                        f"共享状态提交锁被占用（{self._commit_timeout}s 内未获取），"
                        f"发布 {release_id} 的 {op_name} 本次未生效，可重试"
                    )
                attempt += 1
                await asyncio.sleep(min(0.01, self._commit_timeout / 10))

    def _commit(
        self,
        release_id: str,
        op_name: str,
        mutator: Callable[[ReleaseRecord, _Txn], object],
        expected_version: int | None,
    ) -> object:
        # 第三步：以盘上最新快照为基准——拒绝用内存旧进度覆盖新结果。
        snap = self._read_latest()
        txn = _Txn(records=dict(snap.records), quarantined=list(snap.quarantined))
        record = txn.records.get(release_id)
        if record is None:
            self._install_snapshot(snap)
            raise ReleaseNotFoundError(f"发布不存在: {release_id}")

        # 第四步：乐观版本校验（调用方明确携带 expected_version 时才拒绝）。
        if expected_version is not None and record.version != expected_version:
            self._reject_stale(record, txn, op_name, expected_version)

        try:
            result = mutator(record, txn)
        except BaseException:
            # 探测超时/连续失败等路径已在抛异常前完成回滚并标记 dirty，
            # 必须先落盘再把异常抛给调用方。
            if txn.dirty:
                self._write_snapshot(txn, record)
            else:
                self._install_snapshot(
                    LoadResult(txn.records, txn.quarantined)
                )
            raise
        if txn.dirty:
            self._write_snapshot(txn, record)
        else:
            self._install_snapshot(LoadResult(txn.records, txn.quarantined))
        return result

    def _reject_stale(
        self,
        record: ReleaseRecord,
        txn: _Txn,
        op_name: str,
        expected_version: int,
    ) -> None:
        """陈旧版本拒绝：调用不生效，但原因写入事件链与查询（version 也前进）。"""
        now = self._time()
        pre_version = record.version
        self._append_event(record, "operation_rejected", {
            "reason": "stale_version",
            "op": op_name,
            "expected_version": expected_version,
            "current_version": pre_version,
            "current_status": record.status.value,
            "current_batch": record.current_batch,
            "decision": (
                f"调用方持有版本 v{expected_version}，共享状态已到 v{pre_version}"
                f"（{record.status.value}）：拒绝用旧进度覆盖，本次{op_name}未生效"
            ),
        })
        record.updated_at = now
        txn.persist()
        self._write_snapshot(txn, record)
        logger.warning(
            "stale rejected: instance=%s id=%s op=%s expected=v%s current=v%s status=%s",
            self._instance_id, record.id, op_name,
            expected_version, record.version, record.status.value,
        )
        exc = StaleVersionError(
            f"发布 {record.id} 的共享状态已变更（期望版本 v{expected_version}，"
            f"当前 v{record.version}，状态 {record.status.value}），{op_name} 未生效"
        )
        exc.expected_version = expected_version  # type: ignore[attr-defined]
        exc.current_version = record.version  # type: ignore[attr-defined]
        exc.record = record  # type: ignore[attr-defined]
        raise exc

    def _read_latest(self) -> LoadResult:
        """读盘上最新快照；文件不可读/不兼容时直接失败，绝不回写覆盖。"""
        try:
            return self.store.read_snapshot()
        except IncompatibleStateError:
            logger.error(
                "shared state incompatible, refuse to overwrite: instance=%s",
                self._instance_id, exc_info=True,
            )
            raise
        except OSError as exc:
            logger.error("shared state unreadable, refuse to overwrite: %s", exc)
            raise IncompatibleStateError(f"共享状态文件不可读，拒绝写入: {exc}") from exc

    def _write_snapshot(self, txn: _Txn, changed: ReleaseRecord) -> None:
        """原子落盘一次并刷新缓存。调用方必须已持有全局提交锁。

        所有生效变更（含陈旧拒绝事件）在此处统一让目标记录 version 单调 +1：
        一次逻辑操作只提交一次，故只 +1。
        """
        changed.version += 1
        self.store.save_all(txn.records, txn.quarantined)
        self._install_snapshot(LoadResult(txn.records, txn.quarantined))
        logger.debug(
            "committed: instance=%s id=%s version=v%s status=%s",
            self._instance_id, changed.id, changed.version, changed.status.value,
        )

    # ------------------------------------------------------------------ #
    # 推进（核心状态机）
    # ------------------------------------------------------------------ #

    async def advance(
        self, release_id: str, *, expected_version: int | None = None
    ) -> tuple[ReleaseRecord, GateAction]:
        return await self._with_exclusive(
            release_id, "advance", self._advance_state, expected_version
        )

    def _advance_state(
        self, record: ReleaseRecord, txn: _Txn
    ) -> tuple[ReleaseRecord, GateAction]:
        status = record.status
        logger.info(
            "advance input: instance=%s id=%s version=v%s status=%s current_batch=%d "
            "weight=%d active=%s",
            self._instance_id, record.id, record.version, status.value,
            record.current_batch, record.current_weight, record.active_version,
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
            return self._start_first_batch(record, now, txn)

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
            record.updated_at = now
            txn.persist()
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
        return self._run_gate(record, batch, now, txn)

    def _start_first_batch(
        self, record: ReleaseRecord, now: float, txn: _Txn
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
        txn.persist()
        logger.info(
            "state change: id=%s PENDING -> IN_PROGRESS batch=1 weight=%d",
            record.id, batch.weight,
        )
        return record, GateAction.STARTED

    def _run_gate(
        self, record: ReleaseRecord, batch: BatchState, now: float, txn: _Txn
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
            self._rollback(record, f"健康探测超时（第 {batch.gate_attempts} 次尝试）", now, txn)
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
            txn.persist()
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
                    now, txn,
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
            txn.persist()
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
            txn.persist()
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
        txn.persist()
        logger.info(
            "state change: id=%s gate HEALTHY -> batch=%d weight=%d",
            record.id, next_index, next_batch.weight,
        )
        return record, GateAction.ADVANCED

    # ------------------------------------------------------------------ #
    # 回滚 / 中止 / 恢复
    # ------------------------------------------------------------------ #

    def _rollback(
        self, record: ReleaseRecord, reason: str, now: float, txn: _Txn
    ) -> None:
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
        txn.persist()
        logger.error(
            "state change: id=%s -> ROLLED_BACK active=%s weight=0 reason=%s",
            record.id, record.plan.stable, reason,
        )

    async def abort(
        self, release_id: str, reason: str = "", *, expected_version: int | None = None
    ) -> ReleaseRecord:
        return await self._with_exclusive(
            release_id, "abort",
            lambda rec, txn: self._abort_state(rec, reason, txn),
            expected_version,
        )

    def _abort_state(
        self, record: ReleaseRecord, reason: str, txn: _Txn
    ) -> ReleaseRecord:
        now = self._time()
        if record.status == ReleaseStatus.ABORTED:
            raise DuplicateAdvanceError(f"发布 {record.id} 已处于中止状态")
        if record.status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
            raise IllegalTransitionError(
                f"发布 {record.id} 处于终态 {record.status.value}，不允许中止"
            )
        record.status = ReleaseStatus.ABORTED
        record.rollback_reason = reason or None
        record.updated_at = now
        self._append_event(record, "manual_abort", {
            "reason": reason, "candidate_weight": record.current_weight,
            "active_version": record.active_version,
            "decision": "人工中止：冻结当前流量，不再推进/自动回滚",
        })
        txn.persist()
        logger.warning(
            "state change: instance=%s id=%s -> ABORTED weight=%d active=%s reason=%s",
            self._instance_id, record.id, record.current_weight,
            record.active_version, reason,
        )
        return record

    async def resume(
        self, release_id: str, *, expected_version: int | None = None
    ) -> ReleaseRecord:
        return await self._with_exclusive(
            release_id, "resume", self._resume_state, expected_version
        )

    def _resume_state(self, record: ReleaseRecord, txn: _Txn) -> ReleaseRecord:
        now = self._time()
        if record.status == ReleaseStatus.ABORTED:
            raise ManualAbortError(
                f"发布 {record.id} 已被人工中止，不可恢复（请新建发布）"
            )
        if record.status in (ReleaseStatus.COMPLETED, ReleaseStatus.ROLLED_BACK):
            raise IllegalTransitionError(
                f"发布 {record.id} 处于终态 {record.status.value}，不允许恢复"
            )
        if record.status != ReleaseStatus.PAUSED:
            raise IllegalTransitionError(
                f"发布 {record.id} 当前为 {record.status.value}，仅暂停态可恢复"
            )
        record.status = ReleaseStatus.IN_PROGRESS
        batch = record.batches[record.current_batch - 1]
        batch.started_at = now  # 恢复后重新计算观察期
        record.updated_at = now
        self._append_event(record, "resumed", {
            "batch": record.current_batch, "candidate_weight": record.current_weight,
            "decision": "人工恢复：观察期重新计时，下一次 advance 将重新过门禁",
        })
        txn.persist()
        logger.info(
            "state change: id=%s PAUSED -> IN_PROGRESS batch=%d weight=%d",
            record.id, record.current_batch, record.current_weight,
        )
        return record

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #

    def _append_event(self, record: ReleaseRecord, event_type: str, detail: dict) -> None:
        event = {
            "seq": len(record.events) + 1,
            "timestamp": self._time(),
            "type": event_type,
            "instance_id": self._instance_id,
            "detail": detail,
        }
        record.events.append(event)
        logger.info("event id=%s seq=%d type=%s instance=%s detail=%s",
                    record.id, event["seq"], event_type, self._instance_id, detail)
