"""跨进程文件锁（骨架：仅类型定义与空实现）。

为多实例共享同一份状态文件提供：
- 每发布一把锁：同一发布的推进/恢复/中止跨进程串行，抢不到立即失败；
- 一把全局状态锁：串行化状态文件的读-改-写，避免不同发布互相覆盖。
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import logging
import os
import time
from pathlib import Path
from typing import Iterator

logger = logging.getLogger("release_orchestrator.locking")


class LockBusy(RuntimeError):
    """锁在限定时间内未能获取。"""


class FileLock:
    """基于 fcntl.flock 的跨进程互斥锁（每次打开独立文件描述符）。

    - flock 与"打开文件描述"关联：同一进程内不同线程/协程各自 open 后，
      彼此同样互斥（不共享可重入语义），因此无需额外的进程内 asyncio 锁；
    - 持锁进程异常退出时由内核自动释放，不会残留死锁（锁文件可留作复用）。
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def acquire_nb(self) -> int:
        """非阻塞获取独占锁，返回持有的文件描述符；抢不到抛 LockBusy。

        与 release_fd 配对使用，供需要在多个 await/调用边界之间持锁的场景。
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                raise LockBusy(f"锁被占用: {self.path.name}") from exc
            raise
        logger.debug("lock acquired (fd): %s", self.path.name)
        return fd

    def release_fd(self, fd: int) -> None:
        """释放并关闭 acquire_nb 返回的文件描述符（即释放锁）。"""
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            logger.exception("lock release failed: %s", self.path.name)
        finally:
            os.close(fd)

    @contextlib.contextmanager
    def hold(
        self,
        *,
        wait_timeout: float | None = None,
        poll_interval: float = 0.01,
    ) -> Iterator[None]:
        """持有锁的上下文管理器。

        wait_timeout=None 表示非阻塞：抢不到立即抛 LockBusy；
        否则在超时时间内轮询重试，超时抛 LockBusy。
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o644)
        acquired = False
        try:
            if wait_timeout is None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    if exc.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                        raise LockBusy(f"锁被占用: {self.path.name}") from exc
                    raise
            else:
                deadline = time.monotonic() + max(0.0, wait_timeout)
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except OSError as exc:
                        if exc.errno not in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                            raise
                        if time.monotonic() >= deadline:
                            raise LockBusy(
                                f"等待锁超时（{wait_timeout}s）: {self.path.name}"
                            ) from exc
                        time.sleep(poll_interval)
            acquired = True
            logger.debug("lock acquired: %s", self.path.name)
            yield
        finally:
            if acquired:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    logger.exception("lock release failed: %s", self.path.name)
            os.close(fd)


class LockManager:
    """锁文件布局：<状态文件目录>/.locks/<release-id>.lock 与 global.state.lock。"""

    GLOBAL_LOCK_NAME = "global.state"

    def __init__(self, state_path: str | Path) -> None:
        self._locks_dir = Path(state_path).parent / ".locks"

    def _lock_path(self, name: str) -> Path:
        return self._locks_dir / f"{name}.lock"

    def for_release(self, release_id: str) -> FileLock:
        """某发布的独立锁（不同发布互不阻塞）。"""
        return FileLock(self._lock_path(f"release-{release_id}"))

    def global_state(self) -> FileLock:
        """状态文件读-改-写全局锁。"""
        return FileLock(self._lock_path(self.GLOBAL_LOCK_NAME))
