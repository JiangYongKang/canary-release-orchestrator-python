"""本地健康探针与门禁判定。

不依赖任何外部服务：
- 测试 / 调用方通过 ``inject_healthy / inject_unhealthy / inject_timeout``
  预置样本，或注册本地 ``probe`` 可调用对象（可抛 ``TimeoutError`` 模拟超时）。
- 判定规则：
    * 健康样本        -> CONTINUE（清零连续失败计数）
    * 不健康且未达阈值 -> PAUSE（保留当前权重，等待人工处置）
    * 连续不健康达阈值 -> 抛 ConsecutiveFailureError（编排器据此自动回滚）
    * 探测超时         -> 抛 ProbeTimeoutError（编排器据此自动回滚）

两类异常各自独立、可识别（code 不同），对应不同的回滚原因。
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable
from typing import Deque

from .errors import ConsecutiveFailureError, ProbeTimeoutError
from .models import (
    GateDecision,
    GateEvaluation,
    HealthSample,
)

logger = logging.getLogger("canary.health")

Probe = Callable[[str, str], HealthSample | bool]
"""探针签名：(release_id, version) -> 健康样本或布尔值；可抛 TimeoutError。"""

_TIMEOUT = object()  # 注入“探测超时”样本的哨兵


class HealthGate:
    """依据本地注入的健康样本给出门禁判定。"""

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        timeout_seconds: float = 1.0,
        probe: Probe | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.probe = probe or (lambda release_id, version: HealthSample(healthy=True, reason="默认本地探针：健康"))
        self._monotonic = monotonic
        self._queues: dict[str, Deque[object]] = defaultdict(deque)
        self._fail_counts: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    # -------------------------------------------------------------- 注入接口

    def inject_healthy(self, release_id: str, reason: str = "指标正常") -> None:
        with self._lock:
            self._queues[release_id].append(HealthSample(healthy=True, reason=reason))

    def inject_unhealthy(self, release_id: str, reason: str = "指标异常") -> None:
        with self._lock:
            self._queues[release_id].append(HealthSample(healthy=False, reason=reason))

    def inject_timeout(self, release_id: str) -> None:
        with self._lock:
            self._queues[release_id].append(_TIMEOUT)

    def reset(self, release_id: str | None = None) -> None:
        """清空注入队列与失败计数（测试辅助）。"""
        with self._lock:
            if release_id is None:
                self._queues.clear()
                self._fail_counts.clear()
            else:
                self._queues.pop(release_id, None)
                self._fail_counts.pop(release_id, None)

    # -------------------------------------------------------------- 门禁判定

    def evaluate(self, release_id: str, version: str) -> GateEvaluation:
        """对当前批次做一次门禁判定，返回结构化结论或抛出可识别异常。"""
        sample = self._next_sample(release_id, version)

        logger.info(
            "[gate] release=%s version=%s input healthy=%s reason=%s",
            release_id,
            version,
            sample.healthy,
            sample.reason,
        )

        if sample.healthy:
            with self._lock:
                self._fail_counts[release_id] = 0
                count = 0
            decision = GateEvaluation(
                decision=GateDecision.CONTINUE,
                reason=f"健康检查通过：{sample.reason}",
                consecutive_failures=count,
                sample=sample,
            )
            logger.info("[gate] release=%s 判定=CONTINUE 依据=%s", release_id, decision.reason)
            return decision

        with self._lock:
            self._fail_counts[release_id] += 1
            count = self._fail_counts[release_id]
        if count >= self.failure_threshold:
            err = ConsecutiveFailureError(
                f"连续 {count} 次健康检查失败（阈值 {self.failure_threshold}），触发回滚",
                release_id=release_id,
            )
            logger.warning("[gate] release=%s 判定=ROLLBACK 依据=%s", release_id, err.message)
            raise err

        decision = GateEvaluation(
            decision=GateDecision.PAUSE,
            reason=(
                f"健康检查未通过：{sample.reason}；连续失败 {count}/"
                f"{self.failure_threshold}，暂停发布等待人工处置"
            ),
            consecutive_failures=count,
            sample=sample,
        )
        logger.info("[gate] release=%s 判定=PAUSE 依据=%s", release_id, decision.reason)
        return decision

    # -------------------------------------------------------------- 内部

    def _next_sample(self, release_id: str, version: str) -> HealthSample:
        """取下一条注入样本；无注入则调用本地探针，并处理超时链路。"""
        with self._lock:
            queue = self._queues[release_id]
            injected = queue.popleft() if queue else None

        if injected is _TIMEOUT:
            err = ProbeTimeoutError(
                f"对版本 {version} 的健康探测超时（注入模拟）",
                release_id=release_id,
            )
            logger.warning("[gate] release=%s 判定=ROLLBACK 依据=%s", release_id, err.message)
            raise err

        if injected is not None:
            assert isinstance(injected, HealthSample)
            return injected

        # 无注入：执行本地探针，测量真实耗时，同时识别探针抛出的 TimeoutError
        start = self._monotonic()
        try:
            result = self.probe(release_id, version)
        except TimeoutError as exc:
            err = ProbeTimeoutError(
                f"对版本 {version} 的健康探测超时：{exc}",
                release_id=release_id,
            )
            logger.warning("[gate] release=%s 判定=ROLLBACK 依据=%s", release_id, err.message)
            raise err
        elapsed = self._monotonic() - start
        if elapsed > self.timeout_seconds:
            err = ProbeTimeoutError(
                f"对版本 {version} 的健康探测耗时 {elapsed:.3f}s 超过超时 {self.timeout_seconds}s",
                release_id=release_id,
            )
            logger.warning("[gate] release=%s 判定=ROLLBACK 依据=%s", release_id, err.message)
            raise err
        if isinstance(result, bool):
            return HealthSample(healthy=result, reason=f"本地探针返回 {result}")
        return result
