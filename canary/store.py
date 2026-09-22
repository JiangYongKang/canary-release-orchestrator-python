"""发布状态持久化：JSON 文件 + 进程锁 + fcntl 文件锁。

设计要点：
- 每次读改写都在 ``transaction`` 中完成：进程内 ``threading.Lock`` 串行化同进程
  线程（fcntl 的进程级语义对同进程线程不可靠），``fcntl.flock`` 串行化跨进程访问。
- 写入采用临时文件 + ``os.replace`` 原子替换，避免崩溃产生半截文件。
- 启动加载时遇到损坏 / 版本过新 / 单条无法校验的数据，不阻止服务启动，
  而是原样写入 quarantine 目录并在内存中保留可查询的告警（明确标记，绝不静默忽略）。
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from .models import SCHEMA_VERSION, ReleaseState, utcnow

T = TypeVar("T")


class JsonStateStore:
    """基于单个 JSON 文件的发布状态存储。"""

    def __init__(self, path: str | os.PathLike = "data/releases.json") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = self.path.with_name(self.path.name + ".lock")
        self._quarantine_dir = self.path.parent / "quarantine"
        self._quarantine_dir.mkdir(parents=True, exist_ok=True)

        self._thread_lock = threading.Lock()
        # 兼容性告警：[{"kind", "reason", "detail", "quarantined_at", "quarantine_file"}]
        self.warnings: list[dict[str, Any]] = []

        # 启动加载一次：发现并隔离不兼容数据，不抛出、不阻止启动
        self._startup_snapshot = self._load_with_quarantine()

    # ------------------------------------------------------------------ 查询

    def load_all(self) -> dict[str, ReleaseState]:
        """加锁读取全部发布状态快照。"""
        with self._locked_file():
            return self._load_with_quarantine()

    def compat_report(self) -> dict[str, Any]:
        """兼容性报告：当前格式版本与被隔离 / 告警的数据。"""
        return {
            "schema_version": SCHEMA_VERSION,
            "warnings": list(self.warnings),
        }

    # ------------------------------------------------------------------ 事务

    def transaction(self, fn: Callable[[dict[str, ReleaseState]], T]) -> T:
        """在独占事务中执行 ``fn(states)`` 并持久化结果。

        ``fn`` 收到可读写的状态映射；其返回值作为事务返回值。
        仅当 ``fn`` 正常返回时写盘；抛异常则回滚（不写盘）。
        """
        with self._thread_lock:
            with self._locked_file():
                states = self._load_with_quarantine()
                result = fn(states)
                self._write_all(states)
                return result

    # ------------------------------------------------------------------ 内部

    def _locked_file(self):
        """返回获取 fcntl 排他锁的上下文管理器。"""

        class _FileLock:
            def __init__(inner_self, lock_path: Path) -> None:
                inner_self.lock_path = lock_path
                inner_self.fd: int | None = None

            def __enter__(inner_self) -> "_FileLock":
                inner_self.fd = os.open(inner_self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
                fcntl.flock(inner_self.fd, fcntl.LOCK_EX)
                return inner_self

            def __exit__(inner_self, exc_type, exc, tb) -> None:
                assert inner_self.fd is not None
                try:
                    fcntl.flock(inner_self.fd, fcntl.LOCK_UN)
                finally:
                    os.close(inner_self.fd)
                    inner_self.fd = None

        return _FileLock(self._lock_path)

    def _load_with_quarantine(self) -> dict[str, ReleaseState]:
        """读取并解析状态文件；不兼容数据隔离到 quarantine。"""
        if not self.path.exists():
            return {}

        raw_text = self.path.read_text(encoding="utf-8")
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            self._quarantine(
                kind="corrupt_json",
                reason=f"状态文件不是合法 JSON，已整体隔离: {exc}",
                raw_text=raw_text,
            )
            return {}

        if not isinstance(data, dict) or not isinstance(data.get("releases"), dict):
            self._quarantine(
                kind="malformed_structure",
                reason="顶层结构不是对象或缺少 releases 映射，已整体隔离",
                raw=data,
            )
            return {}

        file_schema = data.get("schema_version")
        if isinstance(file_schema, int) and file_schema > SCHEMA_VERSION:
            self._quarantine(
                kind="incompatible_schema",
                reason=(
                    f"状态文件 schema_version={file_schema} 新于当前支持的 "
                    f"{SCHEMA_VERSION}，拒绝猜测性解析，已整体隔离"
                ),
                raw=data,
            )
            return {}
        # file_schema < SCHEMA_VERSION 或缺失：按当前版本尽力解析，
        # 单条记录失败仍会被逐条隔离（兼容范围见 README）。

        states: dict[str, ReleaseState] = {}
        for release_id, raw_state in data["releases"].items():
            try:
                state = ReleaseState.model_validate(raw_state)
            except Exception as exc:  # pydantic.ValidationError 等
                self._quarantine(
                    kind="invalid_release_record",
                    reason=f"发布 {release_id!r} 的状态记录无法解析，已隔离该条: {exc}",
                    raw=raw_state,
                )
                continue
            if state.release_id != release_id:
                self._quarantine(
                    kind="invalid_release_record",
                    reason=(
                        f"发布 {release_id!r} 的键与记录内 release_id="
                        f"{state.release_id!r} 不一致，已隔离该条"
                    ),
                    raw=raw_state,
                )
                continue
            states[release_id] = state
        return states

    def _write_all(self, states: dict[str, ReleaseState]) -> None:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "releases": {rid: s.model_dump(mode="json") for rid, s in states.items()},
        }
        tmp_path = self.path.with_name(f".{self.path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def _quarantine(
        self,
        *,
        kind: str,
        reason: str,
        raw: Any = None,
        raw_text: str | None = None,
    ) -> None:
        stamp = utcnow().strftime("%Y%m%dT%H%M%S")
        qfile = self._quarantine_dir / f"{stamp}-{uuid.uuid4().hex[:8]}.json"
        entry = {
            "kind": kind,
            "reason": reason,
            "quarantined_at": utcnow().isoformat(),
            "quarantine_file": str(qfile),
        }
        on_disk: dict[str, Any] = dict(entry)
        if raw_text is not None:
            on_disk["raw_text"] = raw_text
        else:
            on_disk["raw"] = raw
        try:
            qfile.write_text(json.dumps(on_disk, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        except OSError:
            # 隔离盘失败也不能阻塞启动；告警中保留原因
            entry["quarantine_file"] = None
        self.warnings.append(entry)
