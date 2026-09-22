"""正常推进与重复请求测试。"""

from __future__ import annotations

from conftest import create_release, register_scripted
from release_orchestrator.errors import DuplicateAdvanceError, IllegalTransitionError
from release_orchestrator.models import GateAction


def test_full_happy_path(client, clock, registry, plan_body):
    """3 批金丝雀：启动 -> 推进 -> 推进 -> 全量完成。"""
    rel = create_release(client, plan_body)
    rid = rel["id"]
    register_scripted(registry, rid)  # 默认始终健康

    # 首次推进：PENDING -> IN_PROGRESS，切到 10%
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["action"] == GateAction.STARTED.value
    assert body["release"]["status"] == "in_progress"
    assert body["release"]["current_weight"] == 10
    assert body["detail"]["active_version"] == "v2.0.0"

    # 观察期未满重复推进 -> 可区分拒绝，权重不变
    r_dup = client.post(f"/releases/{rid}/advance")
    assert r_dup.status_code == 409
    assert r_dup.json()["error_code"] == "duplicate_advance"
    assert client.get(f"/releases/{rid}").json()["current_weight"] == 10

    # 观察期满 -> 门禁通过，推进到 50%
    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200
    assert r.json()["action"] == GateAction.ADVANCED.value
    assert r.json()["release"]["current_weight"] == 50
    assert r.json()["release"]["current_batch"] == 2
    # 第一批已被标记完成，重启后不会重跑
    assert r.json()["release"]["batches"][0]["completed"] is True

    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200
    assert r.json()["action"] == GateAction.ADVANCED.value
    assert r.json()["release"]["current_weight"] == 100
    assert r.json()["release"]["current_batch"] == 3

    # 末批观察期 + 门禁通过 -> 完成
    clock.advance(61)
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200
    body = r.json()
    assert body["action"] == GateAction.COMPLETED.value
    assert body["release"]["status"] == "completed"
    assert body["release"]["current_weight"] == 100
    assert body["release"]["active_version"] == "v2.0.0"
    assert body["release"]["completed_batches"] == 3

    # 终态再推进 -> 非法状态转换
    r = client.post(f"/releases/{rid}/advance")
    assert r.status_code == 409
    assert r.json()["error_code"] == "illegal_transition"


def test_duplicate_create_rejected(client, plan_body):
    """重复提交同名发布 -> duplicate_release，与非法转换可区分。"""
    create_release(client, plan_body)
    r = client.post("/releases", json=plan_body)
    assert r.status_code == 409
    assert r.json()["error_code"] == "duplicate_release"


def test_invalid_plan_rejected(client, plan_body):
    """非法计划（末批非 100、权重非递增）-> invalid_plan。"""
    body = dict(plan_body, weights=[10, 90])
    r = client.post("/releases", json=body)
    assert r.status_code == 422
    assert r.json()["error_code"] == "invalid_plan"

    body = dict(plan_body, weights=[100, 50, 100])
    r = client.post("/releases", json=body)
    assert r.status_code == 422
    assert r.json()["error_code"] == "invalid_plan"


def test_get_nonexistent(client):
    r = client.get("/releases/nope")
    assert r.status_code == 404
    assert r.json()["error_code"] == "release_not_found"


def test_orchestrator_duplicate_advance_error_types(orchestrator, clock):
    """编排器层直接校验异常类型可区分（asyncio.run 包装异步调用）。"""
    import asyncio

    import pytest

    from release_orchestrator.models import ReleasePlan

    plan = ReleasePlan("p", "v2", "v1", [10, 100], 60)
    rec = orchestrator.create_release(plan)
    asyncio.run(orchestrator.advance(rec.id))
    with pytest.raises(DuplicateAdvanceError):
        asyncio.run(orchestrator.advance(rec.id))
    clock.advance(61)
    asyncio.run(orchestrator.advance(rec.id))
    clock.advance(61)
    asyncio.run(orchestrator.advance(rec.id))
    with pytest.raises(IllegalTransitionError):
        asyncio.run(orchestrator.advance(rec.id))
