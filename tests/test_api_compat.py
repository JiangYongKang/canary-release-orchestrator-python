"""服务入口兼容与 HTTP 层并发冒烟。"""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_root_endpoint_compatible():
    """保持已有接口行为：GET / 仍返回 {"Hello": "World"}。"""
    import main

    with TestClient(main.app) as client:
        r = client.get("/")
        assert r.status_code == 200
        assert r.json() == {"Hello": "World"}


def test_http_concurrent_advance_single_winner(tmp_path):
    """通过 HTTP 并发推进：恰好一个有效，其余返回 advance_conflict。"""
    import threading
    from fastapi import FastAPI

    from release_orchestrator.api import router
    from release_orchestrator.health import HealthProbeRegistry
    from release_orchestrator.orchestrator import ReleaseOrchestrator

    app = FastAPI()
    reg = HealthProbeRegistry()
    t = {"now": 1000.0}
    app.state.orchestrator = ReleaseOrchestrator.from_path(
        str(tmp_path / "s.json"), reg, clock=lambda: t["now"]
    )
    app.state.probes = reg
    app.include_router(router)

    with TestClient(app) as client:
        r = client.post("/releases", json={
            "name": "http-conc", "candidate": "v2", "stable": "v1",
            "weights": [10, 100], "min_observe_seconds": 60,
        })
        rid = r.json()["id"]

        results: list = []
        barrier = threading.Barrier(8)

        def hit():
            barrier.wait()
            results.append(client.post(f"/releases/{rid}/advance"))

        threads = [threading.Thread(target=hit) for _ in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        codes = sorted(r.status_code for r in results)
        oks = [r for r in results if r.status_code == 200]
        rejected = [
            r for r in results
            if r.json().get("error_code") in ("advance_conflict", "duplicate_advance")
        ]
        assert len(oks) == 1, [(r.status_code, r.json()) for r in results]
        assert oks[0].json()["action"] == "started"
        # 其余并发调用全部以可区分方式被拒绝（在途冲突 / 观察期未满），
        # 绝不产生第二个有效推进。
        assert len(rejected) == 7, codes
        # 权重只切换一次
        view = client.get(f"/releases/{rid}").json()
        assert view["current_weight"] == 10
        assert view["current_batch"] == 1
        events = client.get(f"/releases/{rid}/events").json()["events"]
        assert sum(1 for e in events if e["type"] == "traffic_switched") == 1
