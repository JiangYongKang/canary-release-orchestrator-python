"""真实多进程 HTTP 集成：两个 uvicorn 实例共享同一状态文件。

用独立 main 模块（tests/_multi_instance_app.py）启动服务，避免触碰默认状态文件。
覆盖：跨进程并发 advance 的单赢家、expected_version 陈旧返回 409 stale_version、
实例标识与 version 出现在响应里、滚动重启后状态一致。
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
APP_MODULE = "tests._multi_instance_app:app"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, state: Path, port: int, ident: str) -> None:
        env = dict(os.environ)
        env["RELEASE_STATE_FILE"] = str(state)
        env["RELEASE_INSTANCE_ID"] = ident
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", APP_MODULE,
             "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.base = f"http://127.0.0.1:{port}"
        self.client = httpx.Client(base_url=self.base, timeout=10.0)

    def wait_ready(self, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if self.client.get("/health").status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.1)
        raise AssertionError(f"server not ready: {self.proc.stdout.read()}")

    def stop(self) -> None:
        self.client.close()
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()


@pytest.fixture
def servers(tmp_path: Path):
    state = tmp_path / "shared_state.json"
    s1 = Server(state, _free_port(), "http-inst-1")
    s2 = Server(state, _free_port(), "http-inst-2")
    try:
        s1.wait_ready(); s2.wait_ready()
        yield s1, s2, state
    finally:
        s1.stop(); s2.stop()


def _create(client: httpx.Client, name: str = "http-multi") -> dict:
    r = client.post("/releases", json={
        "name": name, "candidate": "v2", "stable": "v1",
        "weights": [10, 100], "min_observe_seconds": 0.01,
    })
    assert r.status_code == 201, r.text
    return r.json()


def test_http_two_processes_single_winner_and_stale(servers):
    s1, s2, _ = servers
    rel = _create(s1.client)
    rid = rel["id"]
    assert rel["version"] == 2  # create 后即 v2

    # 两个实例同时推进：只有一个 200 started；另一个必须以可区分方式未生效
    # ——跨进程抢锁失败是 advance_conflict；若两个请求被系统调度成严格串行，
    # 第二个会拿到锁后判"观察期未满"(duplicate_advance)。两种都不产生第二次切换。
    import threading
    out: list[httpx.Response] = []
    barrier = threading.Barrier(2)

    def hit(client):
        barrier.wait()
        out.append(client.post(f"/releases/{rid}/advance"))

    t1 = threading.Thread(target=hit, args=(s1.client,))
    t2 = threading.Thread(target=hit, args=(s2.client,))
    t1.start(); t2.start(); t1.join(); t2.join()
    oks = [r for r in out if r.status_code == 200]
    rejected = [
        r for r in out
        if r.json().get("error_code") in ("advance_conflict", "duplicate_advance")
    ]
    assert len(oks) == 1, [(r.status_code, r.text) for r in out]
    assert len(rejected) == 1, [(r.status_code, r.text) for r in out]
    assert oks[0].json()["action"] == "started"
    started_version = oks[0].json()["release"]["version"]
    assert started_version == 3
    # 若另一个是观察期未满（落了 advance_rejected 事件、version+1），
    # 后续完成后的版本号相应抬高；据此动态确定陈旧版本基线。
    rej_body = rejected[0].json()
    view_after_reject = s2.client.get(f"/releases/{rid}").json()
    assert view_after_reject["status"] == "in_progress"
    assert view_after_reject["current_weight"] == 10  # 权重只切了一次
    print(f"[判定] 另一个并发请求: {rej_body['error_code']}（未产生第二次切换）")

    # 持 started 版本的调用方继续推进：另一个实例已把它推进/完成 -> stale
    time.sleep(0.05)
    s2.client.post(f"/releases/{rid}/advance")  # -> advanced 第 2 批
    time.sleep(0.05)
    s2.client.post(f"/releases/{rid}/advance")  # -> completed
    r = s1.client.post(
        f"/releases/{rid}/advance", json={"expected_version": started_version}
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["error_code"] == "stale_version"
    assert body["current_version"] > started_version
    assert body["release"]["status"] == "completed"

    # 事件链在另一实例上同样可见（共享状态），拒绝有留痕
    events = s2.client.get(f"/releases/{rid}/events").json()["events"]
    rej = [e for e in events if e["type"] == "operation_rejected"]
    assert len(rej) == 1 and rej[0]["detail"]["reason"] == "stale_version"
    assert sum(1 for e in events if e["type"] == "traffic_switched") == 2
    assert sum(1 for e in events if e["type"] == "release_completed") == 1

    # /health 暴露不同实例标识
    assert s1.client.get("/health").json()["instance_id"] == "http-inst-1"
    assert s2.client.get("/health").json()["instance_id"] == "http-inst-2"
    print("[判定] HTTP 双实例：单赢家 started，陈旧推进 409 stale_version，事件共享可见")


def test_http_restart_new_process_keeps_state(servers, tmp_path):
    s1, s2, state = servers
    rel = _create(s1.client, name="restart-http")
    rid = rel["id"]
    r = s1.client.post(f"/releases/{rid}/advance")
    assert r.status_code == 200 and r.json()["action"] == "started"

    # "滚动重启"：再起一个全新进程指向同一状态文件
    s3 = Server(state, _free_port(), "http-inst-3")
    try:
        s3.wait_ready()
        view = s3.client.get(f"/releases/{rid}").json()
        assert view["status"] == "in_progress"
        assert view["current_weight"] == 10
        assert view["version"] == 3
        time.sleep(0.05)
        r = s3.client.post(f"/releases/{rid}/advance")
        assert r.json()["action"] == "advanced"
        time.sleep(0.05)
        r = s3.client.post(f"/releases/{rid}/advance")
        assert r.json()["action"] == "completed"
    finally:
        s3.stop()
    print("[判定] HTTP 新进程从共享文件恢复并接力完成，无重复批次")
