"""JSON 文件持久化：原子写入、schema 校验与不兼容数据隔离。

- 状态库为单个 JSON 文件，写入采用临时文件 + os.replace 原子替换；
- 进程内 threading.Lock 串行化写入，配合编排器的每发布 asyncio 锁保证并发安全；
- 加载时逐记录做 schema/字段校验：
  * 单条记录不兼容 -> 移入 quarantine 并持久化标记，其余记录正常加载；
  * 整个文件损坏或 schema 版本过新 -> 备份为 .quarantine.<时间戳>，以空库启动；
  任何历史脏数据都不会阻止服务启动，也不会被静默忽略（有日志 + /health 可查）。
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import IncompatibleStateError
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
        "version": r.version,
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
        version=_parse_version(rid, d),
    )
    _check_consistency(rec)
    return rec


def _parse_version(rid: str, d: dict[str, Any]) -> int:
    raw_version = d.get("version", 1)
    if not isinstance(raw_version, int) or isinstance(raw_version, bool) or raw_version < 1:
        raise IncompatibleStateError(
            f"record({rid}): version 必须为 >=1 的整数，实际 {raw_version!r}"
        )
    return int(raw_version)


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

class JsonFileStore:
    """基于本地 JSON 文件的存储。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def load(self) -> LoadResult:
        if not self.path.exists():
            logger.info("state file not found at %s: starting empty", self.path)
            return LoadResult(records={}, quarantined=[])
        try:
            raw_text = self.path.read_text(encoding="utf-8")
        except OSError as exc:
            return self._quarantine_whole_file(f"状态文件无法读取: {exc}")
        try:
            raw = json.loads(raw_text)
        except (json.JSONDecodeError, OSError) as exc:
            return self._quarantine_whole_file(f"状态文件无法解析: {exc}")
        if not isinstance(raw, dict):
            return self._quarantine_whole_file("状态文件根节点不是对象")
        version = raw.get("schema_version")
        if version is None:
            return self._quarantine_whole_file("缺少 schema_version（可能是更旧版本数据）")
        if not isinstance(version, int) or isinstance(version, bool):
            return self._quarantine_whole_file(f"schema_version 非法: {version!r}")
        if version > SCHEMA_VERSION:
            return self._quarantine_whole_file(
                f"schema_version={version} 比当前支持版本 {SCHEMA_VERSION} 更新，无法兼容"
            )
        if version < SCHEMA_VERSION:
            # 当前只有 v1；未来版本在此做迁移。无法迁移时隔离。
            return self._quarantine_whole_file(
                f"schema_version={version} 过于陈旧且无可用迁移路径"
            )
        try:
            return self._parse_payload(raw)
        except IncompatibleStateError as exc:
            return self._quarantine_whole_file(str(exc))

    def read_snapshot(self) -> LoadResult:
        """只读最新盘上快照（不做任何隔离/改名副作用）。

        供多实例在持有全局状态锁后重读共享状态：
        - 文件不存在 -> 空快照；
        - 文件损坏 / schema 不兼容 -> 抛 IncompatibleStateError，
          调用方必须中止本次写，绝不能用内存快照覆盖（避免丢数据）。
        """
        if not self.path.exists():
            return LoadResult(records={}, quarantined=[])
        raw_text = self.path.read_text(encoding="utf-8")
        raw = json.loads(raw_text)
        if not isinstance(raw, dict):
            raise IncompatibleStateError("状态文件根节点不是对象")
        version = raw.get("schema_version")
        if not isinstance(version, int) or isinstance(version, bool) or version != SCHEMA_VERSION:
            raise IncompatibleStateError(
                f"状态文件 schema_version={version!r} 与当前版本 {SCHEMA_VERSION} 不一致，"
                "拒绝在其之上写入以防覆盖"
            )
        return self._parse_payload(raw)

    def _parse_payload(self, raw: dict[str, Any]) -> LoadResult:
        """解析 payload 中的记录与隔离清单；单条坏记录进入隔离而非中止整体。"""
        raw_records = raw.get("releases", {})
        if not isinstance(raw_records, dict):
            raise IncompatibleStateError("releases 节点不是对象")

        records: dict[str, ReleaseRecord] = {}
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

        for q in raw.get("quarantine", []):
            if isinstance(q, dict):
                quarantined.append(QuarantinedRecord.from_dict(q))

        if quarantined:
            logger.warning(
                "loaded %d valid records, %d quarantined (marked, not silently ignored)",
                len(records), len(quarantined),
            )
        else:
            logger.info("loaded %d records, no incompatible data", len(records))
        return LoadResult(records=records, quarantined=quarantined)

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

    def save_all(
        self,
        records: dict[str, ReleaseRecord],
        quarantined: list[QuarantinedRecord] | None = None,
    ) -> None:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "updated_at": time.time(),
            "releases": {rid: record_to_dict(r) for rid, r in records.items()},
            "quarantine": [q.to_dict() for q in (quarantined or [])],
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
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
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
                raise
            logger.debug("state persisted: %d records -> %s", len(records), self.path)
