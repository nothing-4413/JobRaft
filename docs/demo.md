# 演示脚本（约 5 分钟）

这是一份面试演示 runbook：每一幕都有可粘贴的命令、要看的输出，以及"这一幕想证明什么"。
下面的观察结果都来自本机实测（2026-10-07，Windows + Docker Desktop 29.7.2 + PostgreSQL 16，
宿主原生双实例；同一批运行的汇总数字见 [`benchmark-results.md`](benchmark-results.md)）。

## 0. 起栈（30 秒）

```bash
docker compose up --build -d
curl -s localhost:8080/healthz          # ok
curl -s localhost:8081/healthz          # ok
```

- 两个 API 实例在 `8080` / `8081`，Prometheus 在 `9090`，Grafana 在 `3000`（`admin` / `local-dev-password`）。
- 容器里设了 `JOBRAFT_API_TOKEN=local-dev-token`，所以除 `/healthz`、`/readyz` 外的请求都要带鉴权头：

```bash
TOKEN='Authorization: Bearer local-dev-token'
API=http://localhost:8080
```

- 想直接讲代码就从 [`../cmd/jobraft/main.go`](../cmd/jobraft/main.go) 的装配顺序开始：
  存储 → 调度器 → API → 选主。

## 1. 延迟与依赖（1 分钟）

展示：依赖任务在前置任务成功之前不会被派发。

```bash
curl -s -X POST $API/tasks -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"id":"demo-dep","name":"echo","delay":3000000000,"retry":{"max_attempts":1}}'
curl -s -X POST $API/tasks -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"id":"demo-child","name":"echo","depends_on":["demo-dep"],"retry":{"max_attempts":1}}'

for status in $(seq 1 3); do
  curl -s $API/tasks/demo-child -H "$TOKEN"; sleep 2
done
```

实测：

| 观察时刻 | `demo-dep` | `demo-child` |
| --- | --- | --- |
| t+0 s | `pending` | `pending` |
| t+2 s | `pending` | `pending` |
| t+5 s | `success` | `success` |

讲点：

- `delay` 的单位是纳秒（`time.Duration` 的整数值），也可以直接用 `run_at` 传 RFC3339 时间。
- `echo` 是二进制内置的进程内 handler（`JOBRAFT_WORKERS` 默认为 4），所以这一幕不需要外部 Worker，
  演示的是"延迟执行 + 依赖闸门"本身。
- 依赖判定在调度器 tick 里做：`internal/scheduler/scheduler.go` 的 `dependenciesReady`，
  失败/缺失的依赖会把任务直接判为 `failed`，并且提交时会拒绝依赖环（`dependency cycle detected through %s`）。

## 2. 外部 Worker 的完整协议（1 分钟）

[`../examples/demo-worker`](../examples/demo-worker) 是一个 80 行的 Worker：注册、心跳、
长轮询认领、handler 执行期间续租、完成后上报。

```bash
go build -o demo-worker ./examples/demo-worker
./demo-worker -api http://localhost:8080 -worker demo-1 -work 2s &

curl -s -X POST $API/tasks -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"name":"greet","payload":{"who":"interviewer"},"retry":{"max_attempts":3}}'
```

Worker 侧输出：

```
[demo-1] claimed task-1791340951713826300-fdd0fcd0-003932 name="greet" attempt=1 lease_until=...
[demo-1] completed task-1791340951713826300-fdd0fcd0-003932
```

讲点：Worker 不需要知道队列实现，只对接 HTTP；认领返回里自带 `lease_token`，
完成时必须回传它，否则 API 会拒绝（防止陈旧的 Worker 覆盖新状态）。

## 3. 杀掉 Worker，看租约恢复（1.5 分钟，这一幕最重要）

```bash
# 让 demo-1 认领一个长任务，然后立刻杀掉它（-work 30s 保证来得及）
TASK=$(curl -s -X POST $API/tasks -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"name":"recover-me","retry":{"max_attempts":3}}' | jq -r .id)
kill %1                                  # 杀掉 demo-1，模拟 Worker 崩溃

while true; do
  curl -s $API/tasks/$TASK -H "$TOKEN" | jq -r '"\(.status) attempts=\(.attempts) err=\(.last_error)"'
  sleep 1
done
```

实测（`JOBRAFT_LEASE_TTL=3s`）：

- 认领瞬间：`status=running worker_id=demo-1 attempts=1`
- 杀掉进程 **2.9 秒**后：`status=retrying attempts=1 last_error=worker lease expired`
- 同时 `jobraft_tasks_lease_expired_total` 从 51 变成 52
- 再起一个 `./demo-worker -worker demo-2 -work 1s`：它以 `attempts=2` 认领并完成，最终 `status=success`

讲点：

- 不能等 Worker 自报失败——它已经死了。恢复靠每个 API 实例的调度 tick 用
  `ListExpired`（走 `jobraft_tasks_running_lease_idx` 部分索引）发现"还在 running 但租约已过期"的任务。
- 任务回到 `retrying` 而不是直接 `failed`，因为重试预算没用完；`attempts` 保持不变说明
  这次失败没有被当作一次真正的执行尝试。
- 这正是"至少一次"的来源：任务可能被两个 Worker 先后执行。所以 handler 必须幂等，
  提交侧的 `Idempotency-Key` 与完成侧的 `lease_token` 条件更新一起把影响收敛。

## 4. 两个实例、一个数据库（1 分钟）

```bash
# 提交给 8080，从 8081 读——同一个存储
curl -s -X POST $API/tasks -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"name":"echo","payload":{"origin":"8080"},"retry":{"max_attempts":1}}'
curl -s localhost:8081/tasks/<id> -H "$TOKEN"

# 杀掉提交它的那个实例，任务仍会被活着的实例派发完成
curl -s -X POST $API/tasks -H "$TOKEN" -H 'content-type: application/json' \
  -d '{"name":"echo","delay":6000000000,"retry":{"max_attempts":1}}'
docker compose stop jobraft-1
curl -s localhost:8081/tasks/<id> -H "$TOKEN"       # 6 秒后 success
docker compose start jobraft-1
```

实测（用宿主原生双实例复现同样的行为）：

- 提交给 18080 的任务，18081 立刻能看到（`status=success payload={"origin":"8080"}`）。
- 提交一个 `delay=6s` 的任务给 18080，然后**杀掉 18080**：18080 拒绝连接，
  而 18081 在 1.5 秒内把它派发执行完（`status=success attempts=1`）。

讲点：一致性边界是 PostgreSQL，两个实例之间没有任何直接通信；`FOR UPDATE SKIP LOCKED`
保证同一个任务只会被一个实例（或一个 Worker）拿走。`GET /cluster` 显示文件注册表里的节点/leader，
它只负责"哪个实例做后台清理与派发"这类角色选择，不参与任务归属的裁决。

## 5. 压测与看板（1 分钟）

```bash
go run ./cmd/jobraft-bench -endpoints http://localhost:8080,http://localhost:8081 \
  -tasks 1000 -workers 8 -submit-parallelism 16
```

输出形如：

```
cleanup_duration=20ms submitted=1000 submit_duration=1.2s completed=1000 elapsed=7.6s throughput=131.10 tasks/s
```

- `cleanup_duration` 是清理上一轮残留的时间，**不在**测量窗口内（这件事本身也是压测工具的一个修复）。
- Grafana 的 Queue Latency P95 面板对应 `jobraft_task_queue_latency_seconds` 直方图。
- 数字的解释、方差与诚实边界见 [`benchmark-results.md`](benchmark-results.md)：同机 15 次运行的中位数
  落在 129–156 tasks/s（总中位 137、区间 118–167），说明 run-to-run 方差比 store 层优化的效果更大。

## 6. 收尾（30 秒）

- `docker compose down`（`-v` 会删掉 PostgreSQL 数据卷）。
- 想讲深一层，直接翻 [`benchmark-results.md`](benchmark-results.md) 的 "Why a claim does not get slower"：
  同一张 10,000 行表的两个 `EXPLAIN (ANALYZE, BUFFERS)` —— `Sort → Seq Scan` 15.557 ms
  对比 `Index Scan using jobraft_tasks_claim_idx` 0.109 ms，以及认领耗时从 34.08 ms 降到 6.98 ms。

## 常见翻车点

- **端口被别的项目占用**：先 `docker ps`。本机上 `saas-*` / `deploy-*` 容器会占 8080、9090。
- **Docker Hub 不可达**：`docker compose up --build` 需要 `golang:1.22-alpine` 与 `alpine:3.20`；
  拉不到就改用 [`benchmark-results.md`](benchmark-results.md) 的 B 方案（宿主原生双实例 + 复用 Compose 的 PostgreSQL）。
- **Windows 临时端口耗尽**：连续多次压测会出现 `only one usage of each socket address`；
  harness 已复用连接（`MaxIdleConnsPerHost=256`）缓解，但仍需等一会儿或重启 Docker Desktop。
- **`go run` 杀不干净**：`go run ./examples/demo-worker` 会派生子进程，
  用 `go build` 出二进制再 `kill` 它的 pid，否则子进程会继续续租，租约恢复那幕就演不出来。
