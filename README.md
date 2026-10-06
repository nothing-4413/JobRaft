# JobRaft

JobRaft 是一个用 Go 编写的小型工作流/任务调度器，按阶段构建。第一阶段包含一个可嵌入的调度器和一个 HTTP API，支持延迟执行、超时、取消、重试以及任务状态查询。

项目刻意聚焦于调度正确性、投递语义、租约、持久化与协调。它不打算成为通用业务平台或功能繁重的前端；这些关注点保持在核心之外，使实现保持为一个有价值的技术系统项目。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| [`cmd/jobraft`](cmd/jobraft/README.md) | 服务入口，装配存储、调度器、API 与选主器。 |
| [`cmd/jobraft-bench`](cmd/jobraft-bench/README.md) | 零依赖的跨实例压测工具。 |
| [`internal/task`](internal/task/README.md) | 领域模型：任务生命周期、重试策略、校验。 |
| [`internal/store`](internal/store/README.md) | `Store` 契约及内存 / JSON 文件 / PostgreSQL 三种后端。 |
| [`internal/scheduler`](internal/scheduler/README.md) | 任务分发、租约、重试、取消、背压、指标。 |
| [`internal/api`](internal/api/README.md) | HTTP 服务器、JSON 端点与鉴权。 |
| [`internal/cluster`](internal/cluster/README.md) | 领导选举与内嵌复制日志共识模型。 |
| [`internal/fileutil`](internal/fileutil/README.md) | 跨平台原子文件替换。 |
| [`pkg/workerclient`](pkg/workerclient/README.md) | 外部 Worker 客户端 SDK。 |
| [`deploy`](deploy/README.md) | Docker Compose、Prometheus 与 Grafana 配置。 |
| [`docs`](docs/README.md) | 架构、API、运维与压测文档。 |

## 运行

```bash
go run ./cmd/jobraft
```

服务默认监听 `:8080`。

创建任务：

```bash
curl -X POST http://localhost:8080/tasks \
  -H 'content-type: application/json' \
  -d '{"name":"echo","payload":{"message":"hello"},"delay": 0, "retry":{"max_attempts":3}}'
```

可能重试请求的客户端应携带 `Idempotency-Key` 请求头。重复的 key 会返回原任务，而不会创建重复任务。

使用 `GET /tasks`、`GET /tasks/{id}` 和 `DELETE /tasks/{id}` 查询、列出或取消任务。设置更大的 `priority` 值可让符合条件的任务优先执行。任务可通过 `depends_on` 声明前置任务 ID；它们只会在所有前置任务成功后运行，若某个前置任务失败或被取消则会自动失败。
将 `schedule` 设置为以纳秒为单位的时长即可创建周期性任务；每次成功运行后它会回到 `pending` 并再次排期。

可通过 `POST /workers` 与 `POST /workers/{id}` 注册并保活 Worker；用 `GET /workers` 查看。
外部 Worker 用 `POST /workers/{id}/claim` 拉取任务，并通过 `POST /tasks/{id}?complete=true` 携带 `worker_id`、`lease_token` 与可选的 `error` 字段确认完成。心跳会续期该 Worker 的活动任务租约。
成功的 Worker 还可以发送一个 JSON `result`；它会被存储在任务上，并通过任务查询/列表 API 返回。SDK 以 `CompleteWithResult` 暴露该能力。
在 claim 请求上加 `?wait=10s` 可进行长轮询（最长 30 秒）。空闲时重试间隔从 50ms 指数退避到 500ms，因此一直拉不到任务的 Worker 不会持续冲击存储；退避不会超过调用方请求的等待时长。
`GET /metrics` 提供 Prometheus 兼容计数器。
该端点还暴露当前 pending、running、retrying 与在线 Worker 的 gauge，供队列压力看板使用。
用 `/healthz` 做存活探针，用 `/readyz` 做就绪探针。
调度器通过 `SetMaxPending` 施加可配置的在途任务上限；超过上限的提交返回 HTTP 429。

要开启领导选举模式，设置 `JOBRAFT_NODE_ID`。内存选主注册表通过 `GET /cluster` 暴露当前节点/领导者；该注册表接口被设计为可替换成 Raft 后端实现，以支持多进程部署。
设置 `JOBRAFT_CLUSTER_FILE` 时，共享 JSON 文件与排他锁提供轻量级跨进程租约选主。网络分区与更大集群应使用基于共识的注册表。

`internal/cluster.ConsensusGroup` 提供内嵌的复制日志状态机与 quorum API，用于确定性测试与本地集成。
它刻意不宣称能在不可信网络上替代完整的 Raft 实现；`ReplicatedLog` 接口是后续接入该传输层的边界。

外部 Worker 可用 `pkg/workerclient` 的 `Client.Run` 处理 register/heartbeat/claim/complete 循环，而无需手工构造 HTTP 请求。
在浏览器打开 `GET /admin` 可获得轻量级实时管理看板。

部署配置：

- `JOBRAFT_ADDR`（默认 `:8080`）
- `JOBRAFT_WORKERS`（默认 `4`）
- `JOBRAFT_STORE`（可选，单进程 JSON 持久化路径）
- `JOBRAFT_DATABASE_URL`（可选，PostgreSQL 连接 URL；优先于 `JOBRAFT_STORE`，并启用多实例原子任务认领）
- `JOBRAFT_NODE_ID`（可选，领导选举身份）
- `JOBRAFT_CLUSTER_FILE`（可选，多进程领导选举的共享注册表文件）
- `JOBRAFT_MAX_PENDING`（可选，在途任务上限）
- `JOBRAFT_LEASE_TTL`（可选，Worker/任务租约时长，例如 `30s`）
- `JOBRAFT_API_TOKEN`（可选，管理与 Worker API 的 bearer/API key；`/healthz` 与 `/readyz` 仍对探针公开）

数值、时长、存储或集群配置的非法值会在启动时报错退出，而不是静默回退到不安全或意外的模式。

用 `docker build -t jobraft .` 构建容器，并用可写的 `/data` 卷运行以实现持久化。

要复现双实例 PostgreSQL 部署，运行 `docker compose up --build`。运维假设与故障注入检查见 `docs/operations.md`。
该栈在 `8080` 与 `8081` 端口暴露 API 实例，Prometheus 在 `9090`，Grafana 在 `3000`（本地使用 `admin` / `local-dev-password`）。
栈就绪后运行 `go run ./cmd/jobraft-bench` 执行 `docs/benchmark-results.md` 中记录的跨实例压测。
架构、故障语义与面试向项目说明见 `docs/architecture.md`。

对已部署实例，设置 `JOBRAFT_API_TOKEN` 并发送 `Authorization: Bearer <token>` 或 `X-API-Key: <token>`。空 token 模式仅用于本地开发。

GitHub Actions 在每次 push 与 pull request 时执行格式化、测试与完整构建。

## 状态机

任务从 `pending` 进入 `running`，再到 `success`。一次失败执行会变为 `retrying`，直到重试预算耗尽，之后变为 `failed`。取消是终态。投递刻意采用至少一次（at-least-once）语义；处理函数应当是幂等的。

存储层是一个接口（`internal/store.Store`），有内存、JSON 文件与 PostgreSQL 三种实现。PostgreSQL 使用事务与 `FOR UPDATE SKIP LOCKED` 实现跨 API 实例的原子认领。默认二进制使用并发安全的内存实现。

当 `JOBRAFT_STORE` 指向一个 JSON 文件时，进程停止时仍在运行的任务会在下次启动时被恢复为可重试的任务。
