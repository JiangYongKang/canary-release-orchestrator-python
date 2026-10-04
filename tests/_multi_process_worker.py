"""多实例测试的子进程入口（必须是独立文件：Python 3.14 默认 forkserver 需要可导入的 __main__）。

协议：单行 JSON 参数（argv[1]），单行 JSON 结果打到 stdout。
所有场景共享同一份状态文件，模拟"多个本地服务进程 + 滚动重启"。
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

sys.path.insert(0, ".")

from release_orchestrator.errors import OrchestratorError  # noqa: E402
from release_orchestrator.health import (  # noqa: E402
    AlwaysHealthyProbe,
    HealthProbeRegistry,
    HealthProbeResult,
    HealthVerdict,
    ScriptedHealthProbe,
)
from release_orchestrator.orchestrator import ReleaseOrchestrator  # noqa: E402
from release_orchestrator.models import ReleasePlan  # noqa: E402


class _SlowProbe:
    """门禁调用固定阻塞若干秒（模拟内部指标 1~2 秒才返回），用于跨进程隔离测试。"""

    def __init__(self, delay: float, verdict: str = "healthy") -> None:
        self.delay = float(delay)
        self.verdict = verdict
        self.calls = 0

    def probe(self, release_id, candidate, weight):
        self.calls += 1
        time.sleep(self.delay)
        return HealthProbeResult(HealthVerdict[self.verdict.upper()], "slow probe")


def _orchestrator(cfg: dict, iid: str) -> ReleaseOrchestrator:
    reg = HealthProbeRegistry()
    orch = ReleaseOrchestrator.from_path(
        cfg["state"], reg,
        clock=(lambda: float(_clock[0])) if cfg.get("fake_clock") else time.time,
        commit_timeout=float(cfg.get("commit_timeout", 5.0)),
        instance_id=iid,
    )
    return orch


_clock = [1_000_000.0]


def _tick(seconds: float = 0.01) -> None:
    """推进共享假时钟（各进程独立时钟仅用于越过观察期，不参与协作判定）。"""
    _clock[0] += max(0.0, seconds)


def _error_payload(exc: Exception) -> dict:
    payload = {"err": getattr(exc, "error_code", type(exc).__name__),
               "message": str(exc)}
    rec = getattr(exc, "record", None)
    if rec is not None:
        payload["status"] = rec.status.value
        payload["version"] = rec.version
        payload["batch"] = rec.current_batch
    return payload


def _run(cfg: dict) -> dict:
    iid = cfg["instance_id"]
    orch = _orchestrator(cfg, iid)
    rid = cfg["release_id"]
    # 每个进程都注册永远健康的探测器（多实例下探测器是各进程本地注入的）
    orch.probes.register(rid, AlwaysHealthyProbe())
    if cfg.get("slow_probe") is not None:
        sp = cfg["slow_probe"]
        orch.probes.register(
            rid, _SlowProbe(float(sp.get("delay", 1.0)), sp.get("verdict", "healthy"))
        )
    if cfg.get("scripted"):
        orch.probes.register(
            rid, ScriptedHealthProbe(
                [HealthProbeResult(HealthVerdict[v.upper()], "scripted")
                 for v in cfg["scripted"]]
            )
        )

    out: list[dict] = []
    for call in cfg["calls"]:
        op = call["op"]
        ev = call.get("expected_version")
        # retry_until_effective：模拟真实调用方对"本次未生效"结果的退避重试，
        # 直到操作生效、到达终态，或命中不可重试的拒绝（stale/非法状态等）。
        # 用于高压压测确定性收敛；冲突/锁忙本身的单赢家语义另有不重试用例覆盖。
        retry = call.get("retry_until_effective", False)
        max_tries = int(call.get("max_tries", 200))
        tries = 0
        while True:
            tries += 1
            # 首个 gate 判定从第 2 次 advance 开始（第 1 次只负责启动首批）；
            # 因此 scripted 序列按"门禁次数"给出，启动调用消费不到判定，需要垫一个占位。
            if cfg.get("fake_clock") and op == "advance":
                _tick(cfg.get("tick_seconds", 61.0))
            try:
                if op == "advance":
                    rec, action = asyncio.run(orch.advance(rid, expected_version=ev))
                    item = {"ok": action.value, "version": rec.version,
                            "status": rec.status.value, "batch": rec.current_batch}
                elif op == "abort":
                    rec = asyncio.run(orch.abort(rid, call.get("reason", ""), expected_version=ev))
                    item = {"ok": "aborted", "version": rec.version,
                            "status": rec.status.value}
                elif op == "resume":
                    rec = asyncio.run(orch.resume(rid, expected_version=ev))
                    item = {"ok": "resumed", "version": rec.version,
                            "status": rec.status.value}
                elif op == "get":
                    rec = orch.get_release(rid)
                    item = {"ok": "get", "version": rec.version,
                            "status": rec.status.value, "batch": rec.current_batch}
                out.append(item)
                break
            except OrchestratorError as exc:
                item = _error_payload(exc)
                retryable = item["err"] in ("advance_conflict", "duplicate_advance",
                                            "state_commit_busy")
                if retry and retryable and tries < max_tries:
                    time.sleep(0.002)
                    continue
                item["tries"] = tries
                out.append(item)
                break
    return {"instance_id": iid, "results": out}


if __name__ == "__main__":
    cfg = json.loads(sys.argv[1])
    try:
        result = _run(cfg)
    except Exception as exc:  # noqa: BLE001 - 回传给驱动进程，测试中可见
        result = {"instance_id": cfg.get("instance_id"),
                  "fatal": type(exc).__name__, "message": str(exc)}
    print(json.dumps(result, ensure_ascii=False))
