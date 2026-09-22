# 渐进式发布编排器（Canary / Blue-Green Release Orchestrator）

一个**完全本地可运行**的 FastAPI 服务，用于模拟金丝雀/蓝绿发布：发布计划与受控推进、
分批流量权重、最小观察期、健康门禁、自动回滚、并发安全与重启恢复。

- 不依赖任何外部服务、真实凭据或付费资源；健康结果通过本地注入或本地探针函数产生。
- 保持原有服务入口与接口行为兼容：`GET /` 仍返回 `{"Hello": "World"}`。
- 状态持久化在本地 JSON 文件（默认 `data/releases.json`），写入为原子替换。

## 快速开始

```bash
# 安装依赖（uv）
uv sync

# 运行测试（会打印输入、状态变化与门禁判定依据）
uv run pytest -s

# 启动服务（uvicorn，FastAPI standard 自带）
uv run uvicorn main:app --port 8000
# 交互式 API 文档： http://127.0.0.1:8000/docs
```

环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `CANARY_STATE_FILE` | `data/releases.json` | 状态持久化文件路径 |
| `CANARY_FAILURE_THRESHOLD` | `3` | 连续健康失败多少次触发自动回滚 |
| `CANARY_PROBE_TIMEOUT` | `1.0` | 本地探针耗时超过多少秒视为探测超时 |
| `CANARY_LOG_LEVEL` | `INFO` | 日志级别 |

## 核心概念

- **发布计划 `ReleasePlan`**：候选版本、稳定版本、分批流量权重（严格递增、末项必须 100）、
  最小观察期秒数；候选版本与稳定版本不能相同。
- **发布状态 `ReleaseState`**：含状态机位置、已完成批次数、当前候选流量权重、
  每个批次的执行记录、最近门禁结论与回滚原因；是重启恢复的唯一事实来源。
- **权重一致性**：任意时刻 `候选权重 + 稳定权重 = 100`，响应中以 `weight_split` 显式给出。

### 发布状态流转

```
                 create
                   │
                   ▼
              ┌─────────┐  advance(切第1档权重, 观察期满, 门禁通过)
              │ PENDING │──────────────────────────────────────────┐
              └─────────┘                                            │
                   │ abort                                           ▼
                   ▼                                          ┌──────────────┐  门禁暂停   ┌────────┐
              ┌─────────┐                                     │ PROGRESSING │◄──resume───►│ PAUSED │
              │ ABORTED │◄──────────── abort ─────────────────┤            │             └────────┘
              └─────────┘                                     └──────────────┘                  │
                   ▲                                                  │  │  │                    │
                   │ abort（同时把流量收回稳定版本）                    │  │  │ rollback/abort    │ rollback
                   │                                                  │  │  ▼                    ▼
                   │                                  全部门禁通过     │  │ ┌──────────┐    ┌─────────────┐
                   └──────────────────────────────────────────────────┘  └►│ COMPLETED │    │ ROLLED_BACK │
                                                                            └──────────┘    └─────────────┘
```

合法转换由 `ReleaseStatus.can_transition_to` 集中约束：

- `PENDING → PROGRESSING | ABORTED`
- `PROGRESSING → PAUSED | COMPLETED | ROLLED_BACK | ABORTED`
- `PAUSED → PROGRESSING | ROLLED_BACK | ABORTED`
- `COMPLETED / ROLLED_BACK / ABORTED` 为终态，任何转换都被拒绝。

非法转换返回 `409 illegal_transition`；对已中止发布再推进返回独立的
`409 manual_abort` 链路，便于区分“普通非法动作”和“人工中止语义”。

### 受控推进与最小观察期（两阶段）

调用 `POST /releases/{id}/advance` 推进，单个批次分两阶段：

1. **启动批次**：把候选流量切到该批次权重（如 20%），记录 `started_at` 并立即落盘，
   返回 `action=batch_started`。
2. **观察 + 门禁**：观察时间必须 `>= min_observation_seconds`。
   - 观察期内再次调用：拒绝，返回 `425 observation_pending`（含 `remaining_seconds`），
     权重与状态不变。
   - 观察期满再调用：执行健康门禁，按结论 continue / pause / rollback 处置。
   - `min_observation_seconds=0` 时，两个阶段在同一次调用内完成。

每完成一个批次 `completed_batches += 1` 并落盘；**已完成批次绝不重复执行**。
全部门禁通过后状态置为 `COMPLETED`，候选版本权重 100% 且成为对外版本。

## 健康门禁与异常处置规则

门禁结果由本地提供，无任何外部依赖：

- **注入样本**（测试/演示）：`POST /releases/{id}/health-injection`，
  `outcome` 为 `healthy` / `unhealthy` / `timeout`，按调用顺序被消费。
- **本地探针函数**（代码接入）：`HealthGate(probe=fn)`，签名
  `fn(release_id, version) -> HealthSample | bool`；探针可抛 `TimeoutError`
  或真实耗时超过 `CANARY_PROBE_TIMEOUT`，两者等价于探测超时。
- 无注入且未注册探针时，使用“默认健康”的内置探针。

判定规则：

| 观测 | 判定 | 后续 |
| --- | --- | --- |
| 健康 | `CONTINUE`（清零连续失败计数） | 完成当前批次 |
| 不健康且未达阈值 | `PAUSE`，**权重保持在当前批次**，等待人工 resume/rollback/abort | 状态 `PAUSED` |
| 连续不健康达到阈值（默认 3） | 抛 `consecutive_failures` 链路（HTTP 502） | **同事务自动回滚** |
| 探测超时（注入/抛错/真实超时） | 抛 `probe_timeout` 链路（HTTP 504） | **同事务自动回滚** |

三类异常链路的错误码互不相同：`probe_timeout`、`consecutive_failures`、`manual_abort`。
自动回滚在**同一个持锁事务**内完成并落盘；错误响应体会附带回滚后的完整状态
`state_after`、`weight_split` 与 `rolled_back=true`，即使客户端随后重试也只会拿到
终态拒绝（`illegal_transition`），不会重复切换。

### 回滚后的一致状态

回滚（自动或人工）后发布进入 `ROLLED_BACK` 终态：

- 候选权重 `0%`、稳定权重 `100%`（二者之和恒为 100）；
- `active_version` 回到 `stable_version`；
- `last_failure` 记录异常类型（`probe_timeout` / `consecutive_failures` / `manual_abort`），
  `rollback_reason` 记录人类可读原因。

人工 `abort` 进入 `ABORTED` 终态，同样把流量收回稳定版本；重复 abort 幂等
（返回 `action=already_aborted`，状态不改变）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 原有接口，返回 `{"Hello": "World"}`（保持兼容） |
| POST | `/releases` | 创建发布计划，返回 201 与 `release_id` |
| GET | `/releases` | 列出全部发布状态 |
| GET | `/releases/{id}` | 查询单个发布状态 |
| POST | `/releases/{id}/advance` | 受控推进一个批次阶段 |
| POST | `/releases/{id}/pause` | 人工暂停（仅 PROGRESSING 合法） |
| POST | `/releases/{id}/resume` | 人工恢复（仅 PAUSED 合法，恢复后重新起观察期） |
| POST | `/releases/{id}/rollback` | 人工回滚，body 可带 `reason` |
| POST | `/releases/{id}/abort` | 人工中止（流量收回稳定版本，重复调用幂等） |
| POST | `/releases/{id}/health-injection` | 注入下一次门禁结果 `healthy/unhealthy/timeout` |
| GET | `/releases/compat` | 状态数据兼容性报告（schema 版本、被隔离记录） |

可区分的拒绝结果（响应体统一含 `error` / `message` / `release_id`）：

| error 码 | HTTP | 含义 |
| --- | --- | --- |
| `duplicate_submission` | 409 | 相同 `release_id` 的计划重复提交 |
| `illegal_transition` | 409 | 当前状态不允许该动作（含终态重复推进） |
| `manual_abort` | 409 | 发布已人工中止（终态），拒绝推进 |
| `observation_pending` | 425 | 批次仍在最小观察期，含 `remaining_seconds` |
| `consecutive_failures` | 502 | 连续健康失败达阈值，已自动回滚 |
| `probe_timeout` | 504 | 健康探测超时，已自动回滚 |
| `validation_error` | 422 | 计划/参数不满足约束（权重、版本等） |

## 并发安全与持久化

- 每次读改写都在 `JsonStateStore.transaction()` 中完成：
  进程内 `threading.Lock` 串行化同进程线程，`fcntl.flock` 串行化多进程访问；
  写盘采用临时文件 + `fsync` + `os.replace` 原子替换。
- 因此同一发布被并发 `advance` 时**只有一个调用产生有效推进**：
  观察期场景下恰有一个 `batch_started`，其余全部得到 `observation_pending`；
  零观察期场景下每个批次恰好完成一次，多余调用得到终态拒绝。批次不会重复执行，
  状态不会丢失或回退（见 `tests/test_concurrency.py`）。
- 批次“开始切权重”与“门禁结果”分别落盘：即使在两步之间崩溃，重启后也能从
  `completed_batches` 与各批次的 `started_at/completed/gate_decision` 恢复，
  不会重复执行已完成批次。

## 状态数据兼容范围

- 状态文件顶层带 `schema_version`（当前为 `1`）。
- **更旧/缺失版本**：按当前格式尽力解析。
- **更新版本**：拒绝猜测性解析，整体隔离并告警。
- **损坏 JSON / 顶层结构异常**：整体隔离并告警。
- **单条发布记录无法校验**：隔离该条，其他记录照常加载。

任何不兼容数据都：1）不阻止服务启动；2）原样写入状态文件旁的
`quarantine/` 目录；3）出现在内存告警与 `GET /releases/compat` 报告中，
**明确标记而非静默忽略**。

## 本地验证

```bash
# 全部测试：正常推进、重复请求、并发推进、重启恢复、健康失败与回滚、非法转换
uv run pytest -s
```

测试文件：

- `tests/test_happy_path.py`：正常分批推进、观察期拒绝、完成转正
- `tests/test_rejections.py`：重复提交、非法状态转换、人工中止链路
- `tests/test_health_and_rollback.py`：超时、连续失败、暂停/恢复、人工回滚、权重一致性
- `tests/test_concurrency.py`：同发布并发推进只产生一个有效结果
- `tests/test_recovery.py`：重启恢复（已完成批次不重复）、坏数据隔离与兼容报告
- `tests/test_api.py`：HTTP 端到端、原有 `/` 接口兼容、错误码映射

手动 curl 演示：

```bash
CANARY_STATE_FILE=/tmp/demo.json uv run uvicorn main:app --port 8000 &

RID=$(curl -s -X POST localhost:8000/releases -H 'Content-Type: application/json' \
  -d '{"candidate_version":"2.0.0","stable_version":"1.0.0",
       "batch_weights":[20,100],"min_observation_seconds":0}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["release_id"])')

curl -s -X POST localhost:8000/releases/$RID/health-injection \
  -H 'Content-Type: application/json' -d '{"outcome":"timeout"}'
curl -s -X POST localhost:8000/releases/$RID/advance   # 504 probe_timeout，已自动回滚
curl -s localhost:8000/releases/compat                 # 兼容性报告
```

## 目录结构

```
main.py                  # 服务入口、lifespan 装配、异常 -> HTTP 映射（保持 GET /）
canary/models.py         # 状态机、计划/状态/门禁结果等数据模型
canary/errors.py         # 可区分业务异常（各自独立 code / HTTP 状态）
canary/store.py          # JSON 原子持久化、进程锁 + flock、坏数据隔离
canary/health.py         # 本地健康注入/探针、门禁判定（继续/暂停/回滚）
canary/orchestrator.py   # 编排核心：创建、推进、暂停/恢复/回滚/中止
canary/api.py            # FastAPI 路由
tests/                   # pytest 测试（覆盖需求列出的全部场景）
```
