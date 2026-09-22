"""健康门禁：探测协议与本地可注入结果注册表。

仅依赖本机能力：探测器由调用方在本地注入（脚本化判定序列），
编排器不发起任何真实网络调用。
"""

from __future__ import annotations

import logging
from typing import Protocol

from .errors import ProbeTimeoutError
from .models import HealthProbeResult, HealthVerdict

logger = logging.getLogger("release_orchestrator.health")


class HealthProbe(Protocol):
    """健康探测协议。"""

    def probe(
        self,
        release_id: str,
        candidate: str,
        weight: int,
    ) -> HealthProbeResult:
        """对当前批次做一次健康探测。

        约定：
        - 判定通过/降级/不健康 -> 返回对应 HealthProbeResult；
        - 探测本身超时 -> 抛出 ProbeTimeoutError（独立异常链路）。
        """
        ...


class ScriptedHealthProbe:
    """脚本化探测器：按预置判定序列依次返回结果，用于本地验证与测试。

    参数：
    - verdicts: 预置判定序列；probe 次数超过序列长度后重复返回最后一个；
      未提供时默认永远返回 HEALTHY。
    - timeout_at: 第几次探测（1 基）抛出 ProbeTimeoutError，模拟探测超时。
    """

    def __init__(
        self,
        verdicts: list[HealthProbeResult] | None = None,
        *,
        timeout_at: int | None = None,
    ) -> None:
        self._verdicts = list(verdicts or [HealthProbeResult(HealthVerdict.HEALTHY, "default healthy")])
        self._timeout_at = timeout_at
        self._calls = 0

    @property
    def call_count(self) -> int:
        return self._calls

    def probe(self, release_id: str, candidate: str, weight: int) -> HealthProbeResult:
        self._calls += 1
        logger.info(
            "health probe input: release=%s candidate=%s weight=%s call=%d",
            release_id, candidate, weight, self._calls,
        )
        if self._timeout_at is not None and self._calls >= self._timeout_at:
            logger.warning(
                "health decision: PROBE_TIMEOUT release=%s call=%d -> ProbeTimeoutError",
                release_id, self._calls,
            )
            raise ProbeTimeoutError(
                f"健康探测超时: release={release_id} call={self._calls}"
            )
        idx = min(self._calls - 1, len(self._verdicts) - 1)
        result = self._verdicts[idx]
        logger.info(
            "health decision: release=%s call=%d verdict=%s reason=%s metrics=%s",
            release_id, self._calls, result.verdict.value, result.reason, result.metrics,
        )
        return result


class AlwaysHealthyProbe:
    """未注入探测器时的默认实现：本地始终判定健康。"""

    def probe(self, release_id: str, candidate: str, weight: int) -> HealthProbeResult:
        logger.info(
            "health probe input (default): release=%s candidate=%s weight=%s -> healthy",
            release_id, candidate, weight,
        )
        return HealthProbeResult(HealthVerdict.HEALTHY, "default local probe: always healthy")


class HealthProbeRegistry:
    """按 release_id 注册/取出本地探测器。"""

    def __init__(self) -> None:
        self._probes: dict[str, HealthProbe] = {}

    def register(self, release_id: str, probe: HealthProbe) -> None:
        logger.info("probe registered for release=%s probe=%s", release_id, type(probe).__name__)
        self._probes[release_id] = probe

    def get(self, release_id: str) -> HealthProbe:
        return self._probes.get(release_id, _DEFAULT_PROBE)

    def clear(self, release_id: str) -> None:
        self._probes.pop(release_id, None)


_DEFAULT_PROBE = AlwaysHealthyProbe()
