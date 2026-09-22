"""HTTP API 端到端：含原有 "/" 接口兼容性、各错误码、健康注入与自动回滚响应体。"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("CANARY_STATE_FILE", str(tmp_path / "data" / "releases.json"))
    monkeypatch.setenv("CANARY_FAILURE_THRESHOLD", "2")
    import importlib

    import main as main_module
    importlib.reload(main_module)  # 让 lifespan 使用新的环境变量路径
    with TestClient(main_module.app) as c:
        yield c


def create_release(client, weights=(20, 100), obs=0):
    r = client.post("/releases", json={
        "candidate_version": "2.0.0",
        "stable_version": "1.0.0",
        "batch_weights": list(weights),
        "min_observation_seconds": obs,
    })
    assert r.status_code == 201, r.text
    return r.json()["release_id"]


def test_legacy_root_unchanged(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}
    print(f"\n原有接口保持兼容: GET / -> {r.json()}")


def test_create_duplicate_and_illegal_codes(client):
    rid = create_release(client)
    # 重复提交（同计划内容再次创建 -> 不同 id，所以这里测未知资源/终态等更明确）
    assert client.get(f"/releases/{rid}").status_code == 200

    # 不存在的 release
    r = client.post("/releases/nope/advance")
    assert r.status_code == 409 and r.json()["error"] == "illegal_transition"
    print(f"\n未知发布推进: HTTP {r.status_code} error={r.json()['error']}")

    # 非法计划：权重不合法 -> 422（FastAPI 校验）
    r = client.post("/releases", json={
        "candidate_version": "2", "stable_version": "1",
        "batch_weights": [80, 20], "min_observation_seconds": 0,
    })
    assert r.status_code == 422
    print("非法权重计划被 FastAPI 校验拒绝: HTTP 422")


def test_full_flow_via_http_with_injection(client):
    rid = create_release(client, weights=(20, 100), obs=0)

    client.post(f"/releases/{rid}/health-injection", json={"outcome": "healthy"})
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200 and r.json()["action"] == "batch_completed"
    print(f"\n批次0: action=batch_completed, 权重 {r.json()['weight_split']}")

    client.post(f"/releases/{rid}/health-injection", json={"outcome": "healthy"})
    r = client.post(f"/releases/{rid}/advance")
    body = r.json()
    assert body["action"] == "completed"
    assert body["state"]["status"] == "completed"
    print(f"完成: {body['state']['active_version']} 100%")

    # 终态重复推进 -> 409 illegal_transition
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409 and r.json()["error"] == "illegal_transition"
    print(f"终态重复推进: HTTP 409 error={r.json()['error']}")


def test_timeout_injection_triggers_rollback_response(client):
    rid = create_release(client)
    client.post(f"/releases/{rid}/health-injection", json={"outcome": "timeout"})
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 504
    body = r.json()
    assert body["error"] == "probe_timeout"
    assert body["rolled_back"] is True
    assert body["state_after"]["status"] == "rolled_back"
    assert body["weight_split"]["candidate_weight"] == 0
    assert body["weight_split"]["stable_weight"] == 100
    print(f"\n超时链路: HTTP 504 error=probe_timeout；响应携带回滚后状态 "
          f"status=rolled_back stable=100%")


def test_consecutive_failures_http(client):
    rid = create_release(client)
    # 阈值=2：第一次不健康暂停，第二次不健康回滚
    client.post(f"/releases/{rid}/health-injection", json={"outcome": "unhealthy", "reason": "错误率"})
    r = client.post(f"/releases/{rid}/advance")
    assert r.json()["action"] == "paused"
    client.post(f"/releases/{rid}/resume")
    client.post(f"/releases/{rid}/health-injection", json={"outcome": "unhealthy"})
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 502 and r.json()["error"] == "consecutive_failures"
    assert r.json()["state_after"]["status"] == "rolled_back"
    print(f"\n连续失败链路: HTTP 502 error=consecutive_failures -> 自动回滚")


def test_manual_abort_and_compat_endpoint(client):
    rid = create_release(client)
    client.post(f"/releases/{rid}/advance")  # 默认探针健康 -> batch done
    r = client.post(f"/releases/{rid}/abort", json={"reason": "stop"})
    assert r.json()["state"]["status"] == "aborted"
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409 and r.json()["error"] == "manual_abort"
    print(f"\n人工中止后推进: HTTP 409 error=manual_abort（独立链路）")

    r = client.get("/releases/compat")
    assert r.status_code == 200 and r.json()["schema_version"] >= 1
    print(f"兼容性报告: {r.json()}")
