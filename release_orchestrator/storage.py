"""JSON 文件持久化：原子写入、跨进程锁、乐观并发提交、schema 校验与不兼容数据隔离。

多实例协作（多个服务进程共用同一份状态文件）：
- 状态库为单个 JSON 文件，写入采用临时文件 + os.replace 原子替换，任何进程在
  任意时刻读到的都是一份完整状态（不存在半截 JSON）；
- 两把跨进程咨询锁（fcntl.flock，锁文件与状态文件同目录）：
  * 每发布一把操作锁 op-<release_id>.lock：同一发布同时只允许一个进程执行
    变更，抢锁立即失败（非阻塞），由调用方翻译成 advance_conflict；
    不同发布使用不同锁文件，互不排队；
  * 一把全局提交锁 commit.lock：仅在"读最新状态 -> 合并本记录 -> 落盘"的
    极短窗口内持有，串行化不同发布的并发提交，避免全量重写互相覆盖；
- 乐观版本号：顶层 revision 每次提交 +1，每条记录有独立 version（随记录递增）。
  变更必须基于读取时的 version 提交；期间该记录被其他实例改过 -> StaleStateError，
  本次不写入，绝不用旧进度覆盖新结果；
- 历史 v1 数据（无版本号字段）按 version=0 原样加载，首次提交时自动升级，
  schema_version 不变，旧版本程序仍可读取。

加载时逐记录做 schema/字段校验：
  * 单条记录不兼容 -> 移入 quarantine 并持久化标记，其余记录正常加载；
  * 整个文件损坏或 schema 版本过新 -> 备份为 .quarantine.<时间戳>，以空库启动；
  任何历史脏数据都不会阻止服务启动，也不会被静默忽略（有日志 + /health 可查）。
"""

from __future__ import annotations

import contextlib
import copy
import errno
import fcntl
import json
import logging
import os
import re
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import IncompatibleStateError, StorageBusyError
from .models import (
    BatchState,
    ReleasePlan,
    ReleaseRecord,
    ReleaseStatus,
)

logger = logging.getLogger("release_orchestrator.storage")

SCHEMA_VERSION = 1


@dataclass
class QuarantinedRecord:
    """无法兼容、被隔离标记的历史记录。"""

    release_id: str
    reason: str
    raw: dict = field(default_factory=dict)
    quarantined_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "release_id": self.release_id,
            "reason": self.reason,
            "raw": self.raw,
            "quarantined_at": self.quarantined_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "QuarantinedRecord":
        return cls(
            release_id=str(d.get("release_id", "<unknown>")),
            reason=str(d.get("reason", "unknown")),
            raw=d.get("raw") if isinstance(d.get("raw"), dict) else {},
            quarantined_at=float(d.get("quarantined_at", 0.0)),
        )


@dataclass
class LoadResult:
    """启动加载结果：有效记录 + 被隔离记录。"""

    records: dict[str, ReleaseRecord]
    quarantined: list[QuarantinedRecord]


@dataclass
class StateSnapshot:
    """某个时刻共享状态的完整快照（每次读取都深拷贝，调用方可安全修改）。"""

    records: dict[str, ReleaseRecord]
    versions: dict[str, int]          # 每条记录的乐观版本号（旧数据为 0）
    revision: int                     # 顶层版本号，每次提交 +1
    quarantined: list[QuarantinedRecord]


# --------------------------------------------------------------------------- #
# 序列化
# --------------------------------------------------------------------------- #

def plan_to_dict(plan: ReleasePlan) -> dict[str, Any]:
    return {
        "name": plan.name,
        "candidate": plan.candidate,
        "stable": plan.stable,
        "weights": list(plan.weights),
        "min_observe_seconds": plan.min_observe_seconds,
        "max_consecutive_failures": plan.max_consecutive_failures,
    }


def batch_to_dict(b: BatchState) -> dict[str, Any]:
    return {
        "weight": b.weight,
        "completed": b.completed,
        "started_at": b.started_at,
        "completed_at": b.completed_at,
        "gate_attempts": b.gate_attempts,
        "consecutive_failures": b.consecutive_failures,
    }


def record_to_dict(r: ReleaseRecord) -> dict[str, Any]:
    return {
        "id": r.id,
        "plan": plan_to_dict(r.plan),
        "status": r.status.value,
        "current_batch": r.current_batch,
        "batches": [batch_to_dict(b) for b in r.batches],
        "current_weight": r.current_weight,
        "active_version": r.active_version,
        "created_at": r.created_at,
        "updated_at": r.updated_at,
        "completed_batches": r.completed_batches,
        "rollback_reason": r.rollback_reason,
        "events": list(r.events),
    }


def _require(d: dict[str, Any], key: str, types: tuple[type, ...], ctx: str) -> Any:
    if key not in d:
        raise IncompatibleStateError(f"{ctx}: 缺少字段 '{key}'")
    val = d[key]
    if not isinstance(val, types) or isinstance(val, bool) and bool not in types:
        expect = "|".join(t.__name__ for t in types)
        raise IncompatibleStateError(
            f"{ctx}: 字段 '{key}' 类型错误，期望 {expect}，实际 {type(val).__name__}"
        )
    return val


def plan_from_dict(d: Any) -> ReleasePlan:
    ctx = "plan"
    if not isinstance(d, dict):
        raise IncompatibleStateError(f"{ctx}: 不是对象")
    weights = _require(d, "weights", (list,), ctx)
    if not weights or not all(isinstance(w, int) and not isinstance(w, bool) for w in weights):
        raise IncompatibleStateError(f"{ctx}: weights 必须为非空整数列表")
    if any(w <= 0 or w > 100 for w in weights):
        raise IncompatibleStateError(f"{ctx}: weights 每个值必须在 (0,100] 内")
    if any(weights[i] > weights[i + 1] for i in range(len(weights) - 1)):
        raise IncompatibleStateError(f"{ctx}: weights 必须单调不减")
    if weights[-1] != 100:
        raise IncompatibleStateError(f"{ctx}: weights 最后一批必须为 100")
    observe = _require(d, "min_observe_seconds", (int, float), ctx)
    if isinstance(observe, bool) or observe < 0:
        raise IncompatibleStateError(f"{ctx}: min_observe_seconds 必须 >= 0")
    max_fail = d.get("max_consecutive_failures", 3)
    if not isinstance(max_fail, int) or isinstance(max_fail, bool) or max_fail < 1:
        raise IncompatibleStateError(f"{ctx}: max_consecutive_failures 必须为 >=1 的整数")
    return ReleasePlan(
        name=_require(d, "name", (str,), ctx),
        candidate=_require(d, "candidate", (str,), ctx),
        stable=_require(d, "stable", (str,), ctx),
        weights=[int(w) for w in weights],
        min_observe_seconds=float(observe),
        max_consecutive_failures=int(max_fail),
    )


def record_from_dict(d: Any) -> ReleaseRecord:
    ctx = "record"
    if not isinstance(d, dict):
        raise IncompatibleStateError(f"{ctx}: 不是对象")
    rid = str(_require(d, "id", (str,), ctx))
    plan = plan_from_dict(d.get("plan"))
    status_val = _require(d, "status", (str,), f"{ctx}({rid})")
    try:
        status = ReleaseStatus(status_val)
    except ValueError:
        raise IncompatibleStateError(f"{ctx}({rid}): 未知状态 '{status_val}'")
    n = len(plan.weights)
    current_batch = _require(d, "current_batch", (int,), f"{ctx}({rid})")
    if isinstance(current_batch, bool) or not (0 <= current_batch <= n):
        raise IncompatibleStateError(f"{ctx}({rid}): current_batch 越界")
    raw_batches = _require(d, "batches", (list,), f"{ctx}({rid})")
    if len(raw_batches) != n:
        raise IncompatibleStateError(
            f"{ctx}({rid}): batches 长度 {len(raw_batches)} 与 weights 长度 {n} 不一致"
        )
    batches: list[BatchState] = []
    for i, bd in enumerate(raw_batches):
        if not isinstance(bd, dict):
            raise IncompatibleStateError(f"{ctx}({rid}): batches[{i}] 不是对象")
        w = bd.get("weight")
        if not isinstance(w, int) or isinstance(w, bool) or w != plan.weights[i]:
            raise IncompatibleStateError(f"{ctx}({rid}): batches[{i}].weight 与计划不一致")
        batches.append(BatchState(
            weight=int(w),
            completed=bool(bd.get("completed", False)),
            started_at=bd.get("started_at"),
            completed_at=bd.get("completed_at"),
            gate_attempts=int(bd.get("gate_attempts", 0)),
            consecutive_failures=int(bd.get("consecutive_failures", 0)),
        ))
    current_weight = _require(d, "current_weight", (int,), f"{ctx}({rid})")
    if isinstance(current_weight, bool) or not (0 <= current_weight <= 100):
        raise IncompatibleStateError(f"{ctx}({rid}): current_weight 越界")
    active = _require(d, "active_version", (str,), f"{ctx}({rid})")
    events = d.get("events", [])
    if not isinstance(events, list):
        raise IncompatibleStateError(f"{ctx}({rid}): events 不是列表")
    rec = ReleaseRecord(
        id=rid,
        plan=plan,
        status=status,
        current_batch=int(current_batch),
        batches=batches,
        current_weight=int(current_weight),
        active_version=active,
        created_at=float(_require(d, "created_at", (int, float), f"{ctx}({rid})")),
        updated_at=float(_require(d, "updated_at", (int, float), f"{ctx}({rid})")),
        completed_batches=int(d.get("completed_batches", 0)),
        rollback_reason=d.get("rollback_reason"),
        events=events,
    )
    _check_consistency(rec)
    return rec


def _check_consistency(r: ReleaseRecord) -> None:
    """状态内一致性校验：防止历史数据自相矛盾。"""
    done = sum(1 for b in r.batches if b.completed)
    if r.completed_batches != done:
        raise IncompatibleStateError(
            f"record({r.id}): completed_batches={r.completed_batches} 与实际完成批次数 {done} 不一致"
        )
    if r.status == ReleaseStatus.COMPLETED and (done != len(r.batches) or r.current_weight != 100):
        raise IncompatibleStateError(f"record({r.id}): completed 状态与进度/权重不一致")
    if r.status == ReleaseStatus.ROLLED_BACK and (r.current_weight != 0 or r.active_version != r.plan.stable):
        raise IncompatibleStateError(f"record({r.id}): rolled_back 状态与权重/版本不一致")
    if r.status in (ReleaseStatus.IN_PROGRESS, ReleaseStatus.PAUSED, ReleaseStatus.PENDING):
        expected_weight = r.batches[r.current_batch - 1].weight if r.current_batch > 0 else 0
        if r.current_weight != expected_weight:
            raise IncompatibleStateError(
                f"record({r.id}): current_weight={r.current_weight} 与 current_batch 期望 {expected_weight} 不一致"
            )


# --------------------------------------------------------------------------- #
# 存储
# --------------------------------------------------------------------------- #

class _FileLock:
    """单个锁文件上的跨进程咨询锁（fcntl.flock）。

    - 每次获取都用独立的文件描述符：同进程不同线程 / 不同编排器实例也会
      真实互斥（flock 绑定打开文件描述，而非进程）；
    - blocking=False 时抢不到立即返回 False，由上层翻译成"未生效"冲突；
    - 释放在关闭描述符时自动完成，进程异常退出内核也会自动释放，
      不会留下死锁（锁文件本身保留，避免反复建删）。
    """

    def __init__(self, lock_path: Path, *, blocking: bool, timeout: float = 0.0) -> None:
        self._lock_path = lock_path
        self._blocking = blocking
        self._timeout = timeout
        self._fd: int | None = None

    def acquire(self) -> bool:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        # 'a+' 保证锁文件存在；不同调用各自拿到独立 fd
        fd = os.open(str(self._lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        flags = fcntl.LOCK_EX if self._blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        if self._blocking and self._timeout > 0:
            deadline = time.monotonic() + self._timeout
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EAGAIN, errno.EACCES):
                        os.close(fd)
                        raise
                    if time.monotonic() >= deadline:
                        os.close(fd)
                        return False
                    time.sleep(0.005)
        else:
            try:
                fcntl.flock(fd, flags)
            except OSError as exc:
                os.close(fd)
                if exc.errno in (errno.EAGAIN, errno.EACCES) and not self._blocking:
                    return False
                raise
        self._fd = fd
        return True

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None

    def __enter__(self) -> "_FileLock":
        if not self.acquire():
            raise StorageBusyError(f"共享状态锁获取失败: {self._lock_path.name}")
        return self

    def __exit__(self, *exc) -> None:
        self.release()


_SAFE_ID = re.compile(r"[^A-Za-z0-9_.-]")


def _safe_lock_name(release_id: str) -> str:
    return _SAFE_ID.sub("_", release_id)[:120]


class JsonFileStore:
    """基于本地 JSON 文件的存储，支持多进程共享与乐观并发提交。"""

    DEFAULT_COMMIT_TIMEOUT = 10.0

    def __init__(
        self,
        path: str | Path,
        *,
        commit_timeout: float = DEFAULT_COMMIT_TIMEOUT,
    ) -> None:
        self.path = Path(path)
        self.commit_timeout = commit_timeout
        self._lock_dir = self.path.parent / (self.path.name + ".locks")
        # 进程内串行化落盘（跨进程由 commit.lock 保证）
        self._write_lock = threading.Lock()
        # 提交锁支持同线程重入（create_release 在提交锁内复查后直接 commit_record）
        self._commit_local = threading.local()

    # ------------------------------------------------------------------ #
    # 跨进程锁
    # ------------------------------------------------------------------ #

    def try_acquire_release_lock(self, release_id: str) -> _FileLock | None:
        """非阻塞抢占某发布的操作锁；失败返回 None（调用方判定为未生效冲突）。"""
        lock = _FileLock(self._lock_dir / f"op-{_safe_lock_name(release_id)}.lock",
                         blocking=False)
        if lock.acquire():
            return lock
        return None

    @contextlib.contextmanager
    def commit_lock(self, timeout: float | None = None):
        """全局提交锁（阻塞、有界等待）：仅在读-改-写极短窗口持有。

        同线程可重入（flock 本身不可重入，故按线程记录深度，只在最外层真正
        获取/释放跨进程锁）；不同线程仍通过同一 fd 的 flock 真实互斥。
        """
        depth = getattr(self._commit_local, "depth", 0)
        if depth > 0:
            self._commit_local.depth = depth + 1
            try:
                yield
            finally:
                self._commit_local.depth -= 1
            return

        lock = _FileLock(
            self._lock_dir / "commit.lock",
            blocking=True,
            timeout=self.commit_timeout if timeout is None else timeout,
        )
        if not lock.acquire():
            raise StorageBusyError(
                f"共享状态提交锁在 {self.commit_timeout}s 内不可用，本次未生效"
            )
        self._commit_local.depth = 1
        try:
            yield
        finally:
            self._commit_local.depth = 0
            lock.release()

    # ------------------------------------------------------------------ #
    # 读取
    # ------------------------------------------------------------------ #

    def load(self) -> LoadResult:
        snap = self.read_snapshot()
        return LoadResult(records=snap.records, quarantined=snap.quarantined)

    def read_snapshot(self) -> StateSnapshot:
        """读取磁盘上最新的完整状态快照（深拷贝，调用方修改不影响存储层）。

        os.replace 原子替换保证不会读到半截 JSON；若文件恰好无法解析
        （极端的崩溃窗口之外不应该发生），按整体隔离策略处理。
        """
        if not self.path.exists():
            logger.info("state file not found at %s: starting empty", self.path)
            return StateSnapshot(records={}, versions={}, revision=0, quarantined=[])
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            result = self._quarantine_whole_file(f"状态文件无法解析: {exc}")
            return StateSnapshot(
                records={}, versions={}, revision=0, quarantined=result.quarantined
            )
        if not isinstance(raw, dict):
            result = self._quarantine_whole_file("状态文件根节点不是对象")
            return StateSnapshot(
                records={}, versions={}, revision=0, quarantined=result.quarantined
            )
        version = raw.get("schema_version")
        if version is None:
            result = self._quarantine_whole_file("缺少 schema_version（可能是更旧版本数据）")
            return StateSnapshot(
                records={}, versions={}, revision=0, quarantined=result.quarantined
            )
        if not isinstance(version, int) or isinstance(version, bool):
            result = self._quarantine_whole_file(f"schema_version 非法: {version!r}")
            return StateSnapshot(
                records={}, versions={}, revision=0, quarantined=result.quarantined
            )
        if version > SCHEMA_VERSION:
            result = self._quarantine_whole_file(
                f"schema_version={version} 比当前支持版本 {SCHEMA_VERSION} 更新，无法兼容"
            )
            return StateSnapshot(
                records={}, versions={}, revision=0, quarantined=result.quarantined
            )
        if version < SCHEMA_VERSION:
            # 当前只有 v1；未来版本在此做迁移。无法迁移时隔离。
            result = self._quarantine_whole_file(
                f"schema_version={version} 过于陈旧且无可用迁移路径"
            )
            return StateSnapshot(
                records={}, versions={}, revision=0, quarantined=result.quarantined
            )

        raw_records = raw.get("releases", {})
        if not isinstance(raw_records, dict):
            result = self._quarantine_whole_file("releases 节点不是对象")
            return StateSnapshot(
                records={}, versions={}, revision=0, quarantined=result.quarantined
            )

        records: dict[str, ReleaseRecord] = {}
        versions: dict[str, int] = {}
        quarantined: list[QuarantinedRecord] = []
        for key, item in raw_records.items():
            try:
                rec = record_from_dict(item)
            except IncompatibleStateError as exc:
                logger.warning("QUARANTINE record key=%s reason=%s", key, exc)
                quarantined.append(QuarantinedRecord(
                    release_id=str(key),
                    reason=str(exc),
                    raw=item if isinstance(item, dict) else {"_raw": item},
                    quarantined_at=time.time(),
                ))
                continue
            if rec.id in records:
                reason = f"记录 id 重复: {rec.id}"
                logger.warning("QUARANTINE record key=%s reason=%s", key, reason)
                quarantined.append(QuarantinedRecord(
                    release_id=str(key), reason=reason, raw=item if isinstance(item, dict) else {},
                    quarantined_at=time.time(),
                ))
                continue
            records[rec.id] = rec
            # 旧版 v1 数据没有 version 字段：视为 0，首次提交自动升级
            ver = item.get("version", 0) if isinstance(item, dict) else 0
            if not isinstance(ver, int) or isinstance(ver, bool) or ver < 0:
                ver = 0
            versions[rec.id] = ver
            rec.version = ver

        for q in raw.get("quarantine", []):
            if isinstance(q, dict):
                quarantined.append(QuarantinedRecord.from_dict(q))

        revision = raw.get("revision", 0)
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
            revision = 0

        if quarantined:
            logger.warning(
                "loaded %d valid records, %d quarantined (marked, not silently ignored)",
                len(records), len(quarantined),
            )
        else:
            logger.info(
                "loaded %d records revision=%d, no incompatible data",
                len(records), revision,
            )
        return StateSnapshot(
            records=records, versions=versions, revision=revision, quarantined=quarantined
        )

    def get_record(self, release_id: str) -> tuple[ReleaseRecord, int]:
        """读取单条记录的最新磁盘状态及其版本号。"""
        snap = self.read_snapshot()
        rec = snap.records.get(release_id)
        if rec is None:
            raise KeyError(release_id)
        return copy.deepcopy(rec), snap.versions.get(release_id, 0)

    def _quarantine_whole_file(self, reason: str) -> LoadResult:
        """整体文件不可用时：备份原件并明确标记，以空库启动。"""
        backup = self.path.with_suffix(
            self.path.suffix + f".quarantine.{int(time.time())}"
        )
        try:
            os.replace(self.path, backup)
            logger.error(
                "STATE FILE QUARANTINED: %s -> %s reason=%s; starting with empty state",
                self.path, backup, reason,
            )
        except OSError:
            logger.exception(
                "STATE FILE INCOMPATIBLE (backup failed): %s reason=%s",
                self.path, reason,
            )
        q = QuarantinedRecord(
            release_id="<whole-state-file>",
            reason=reason,
            raw={"backup_path": str(backup)},
            quarantined_at=time.time(),
        )
        return LoadResult(records={}, quarantined=[q])

    # ------------------------------------------------------------------ #
    # 提交
    # ------------------------------------------------------------------ #

    def commit_record(
        self,
        record: ReleaseRecord,
        expected_version: int,
        *,
        extra_quarantined: list[QuarantinedRecord] | None = None,
    ) -> int:
        """乐观提交单条记录。

        必须在持有该发布操作锁（或创建场景的提交锁）时调用：
        - 重新读取磁盘最新状态；
        - 若该记录的版本号已不是 expected_version -> 抛 StaleStateError，
          本次内容完全不写入（不覆盖、不回退、不重复完成批次）；
        - 否则合并写回：只替换本记录，其他发布的记录原样保留，
          顶层 revision 与本记录 version 各 +1。

        返回该记录的新版本号。
        """
        from .errors import StaleStateError  # 局部导入避免环形依赖

        with self._write_lock, self.commit_lock():
            snap = self.read_snapshot()
            current_version = snap.versions.get(record.id)
            if current_version is not None and current_version != expected_version:
                logger.warning(
                    "STALE COMMIT rejected: id=%s expected_version=%d actual_version=%d "
                    "(status=%s -> incoming=%s)",
                    record.id, expected_version, current_version,
                    snap.records[record.id].status.value, record.status.value,
                )
                raise StaleStateError(
                    f"发布 {record.id} 的共享状态已被其他实例更新"
                    f"（version {expected_version} -> {current_version}），"
                    f"本次操作基于过期进度，未生效；请重新查询最新状态后再决定"
                )
            record.updated_at = record.updated_at or time.time()
            new_records = dict(snap.records)
            new_versions = dict(snap.versions)
            new_records[record.id] = record
            new_versions[record.id] = (current_version or 0) + 1
            record.version = new_versions[record.id]

            quarantined = self._merge_quarantined(snap.quarantined, extra_quarantined)
            self._write_payload(new_records, new_versions, snap.revision + 1, quarantined)
            logger.info(
                "state persisted: id=%s version=%d revision=%d status=%s weight=%d",
                record.id, new_versions[record.id], snap.revision + 1,
                record.status.value, record.current_weight,
            )
            return new_versions[record.id]

    def commit_quarantined(
        self, quarantined: list[QuarantinedRecord]
    ) -> None:
        """仅追加隔离标记（不改任何发布记录）。"""
        with self._write_lock, self.commit_lock():
            snap = self.read_snapshot()
            merged = self._merge_quarantined(snap.quarantined, quarantined)
            self._write_payload(
                snap.records, snap.versions, snap.revision + 1, merged
            )

    @staticmethod
    def _merge_quarantined(
        existing: list[QuarantinedRecord],
        extra: list[QuarantinedRecord] | None,
    ) -> list[QuarantinedRecord]:
        if not extra:
            return list(existing)
        merged = list(existing)
        seen = {(q.release_id, q.reason) for q in merged}
        for q in extra:
            key = (q.release_id, q.reason)
            if key in seen:
                continue
            seen.add(key)
            merged.append(q)
        return merged

    def _write_payload(
        self,
        records: dict[str, ReleaseRecord],
        versions: dict[str, int],
        revision: int,
        quarantined: list[QuarantinedRecord],
    ) -> None:
        payload_releases: dict[str, dict[str, Any]] = {}
        for rid, rec in records.items():
            d = record_to_dict(rec)
            d["version"] = versions.get(rid, 1)
            payload_releases[rid] = d
        payload = {
            "schema_version": SCHEMA_VERSION,
            "revision": revision,
            "updated_at": time.time(),
            "releases": payload_releases,
            "quarantine": [q.to_dict() for q in quarantined],
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, self.path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise

    def save_all(
        self,
        records: dict[str, ReleaseRecord],
        quarantined: list[QuarantinedRecord] | None = None,
    ) -> None:
        """兼容入口：整体覆盖写入（仅保留给工具/测试，内部提交走 commit_record）。"""
        with self._write_lock, self.commit_lock():
            snap = self.read_snapshot()
            new_versions: dict[str, int] = {}
            for rid, rec in records.items():
                new_versions[rid] = snap.versions.get(rid, 0) + 1
                rec.version = new_versions[rid]
            self._write_payload(
                records, new_versions, snap.revision + 1, quarantined or []
            )
