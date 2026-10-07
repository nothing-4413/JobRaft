# 面试要点与常见追问

这份文档是给自己的准备材料：30 秒概述、90 秒架构、带取舍的决定、以及被追问时的答法。
所有数字都来自仓库里可复现的测量，出处写在对应小节。

## 30 秒概述

JobRaft 是一个用 Go 写的任务调度器：任务可以延迟执行、声明依赖、定时重跑、失败重试；
Worker 通过 HTTP 长轮询认领任务，用租约持有它，用租约令牌做条件写入完成。
多实例部署时的一致性边界是 PostgreSQL（`FOR UPDATE SKIP LOCKED` + 条件更新 + 唯一幂等键），
不依赖共识协议。规模：38 个 Go 文件（15 个源文件、23 个测试文件），约 3,500 行源码与 2,500 行测试。

## 90 秒架构（讲到哪一段就打开哪个文件）

- **提交**：`POST /tasks` → `Scheduler.Submit` → 背压（`CountInFlight` 对比 `JOBRAFT_MAX_PENDING`，
  超限返回 429）→ 依赖存在性与依赖环检查 → `store.Create`。同一个 `Idempotency-Key`
  重复提交会返回原任务而不是新建。
- **状态机**：`pending → running → success`；失败走 `retrying`（按 `RetryPolicy.Backoff` 排下一次
  `run_at`，见 `internal/scheduler/scheduler.go:851`）直到重试预算耗尽变 `failed`；`canceled` 是终态。
  投递语义是至少一次。
- **认领**：外部 Worker 用 `POST /workers/{id}/claim?wait=1s` 长轮询。PostgreSQL 路径在一个事务里
  `SELECT ... WHERE status IN ('pending','retrying') AND run_at <= NOW()
  ORDER BY priority DESC, run_at, id FOR UPDATE SKIP LOCKED LIMIT 64`，
  再用一条 `WHERE id = ANY($1) FOR KEY SHARE` 检查依赖，最后把选中的任务置为 `running`
  并写入 `worker_id` / `lease_token` / `lease_until`。
- **完成**：`POST /tasks/{id}?complete=true` 带 `worker_id` 与 `lease_token`，走 `UpdateIfLease`
  条件更新；令牌或状态不匹配就拒绝，陈旧 Worker 无法覆盖新状态。
- **恢复**：每个实例的调度 tick 用 `ListDue` / `ListExpired`（各自走部分索引）取"已到期"与
  "租约已过期"的任务；过期任务回到 `retrying` 并记录 `worker lease expired`。
- **观测**：`GET /metrics` 给出计数器（submitted / succeeded / failed / retried / canceled /
  lease_expired）、gauge（pending / running / retrying / workers_online）与队列延迟直方图；
  `/admin` 是一个轻量实时看板。

## 五个带取舍的决定

1. **用数据库事务做归属裁决，而不是共识协议。** 一条 `FOR UPDATE SKIP LOCKED` 就给了多实例互斥，
   代价是 PostgreSQL 成为可用性上的关键依赖——但共享状态本来就需要一个唯一的真相来源。
   项目名里的 Raft 只是设计取向：真做网络 Raft 会把时间从"调度语义"挪到"共识实现"，
   对面试项目性价比低，所以我在 README 里明确写了这条边界。
2. **承诺至少一次，而不是恰好一次。** 租约过期意味着任务一定会被重做，
   所以真正要保证的是"不丢"（租约 + `retrying`）和"不重复写状态"（`lease_token` 条件更新
   + 唯一幂等键）。幂等是 handler 的责任，README 与架构文档都写明了。
3. **调度 tick 不加 limit。** `ListDue` 一次读出所有到期任务。加 `LIMIT` 会让排在
   "暂时跑不了"（依赖未满足）的一批任务后面的任务饿死，而延迟与依赖正是这个项目的卖点，
   所以正确性优先于常数因子。
4. **让认领的顺序有索引可用。** 原来的 `jobraft_tasks_due_idx (status, run_at, priority DESC)`
   与 `ORDER BY priority DESC, run_at, id` 不匹配，plan 是 `Sort → Seq Scan`（10,000 行时
   15.557 ms）。加 `jobraft_tasks_claim_idx (priority DESC, run_at, id) WHERE status IN
   ('pending','retrying')` 之后同一查询是 `Index Scan`，0.109 ms，认领耗时从 34.08 ms 降到 6.98 ms。
5. **每一个副作用写入都带前置条件。** 重试、取消、租约回收、队列满回滚都走
   `UpdateIfState` / `UpdateIfLease`；这条规矩是修出来的——队列满时的回滚原先是一句无条件
   `Update`，任务在这期间被取消/回收就会被覆盖。

## 常见追问与答法

- **为什么不用 Redis / Kafka / RabbitMQ？** 它们会替我做掉"认领 + 可见性超时"这件事，
  而项目的目的就是把这件事做出来；用 PostgreSQL 还能顺带讲 `SKIP LOCKED`、部分索引和事务语义。
  真要上规模，Kafka 的分区语义更合适——这是坦白的取舍。
- **Worker 崩溃时怎么办？** `JOBRAFT_LEASE_TTL`（Compose 里是 3 秒）过期后任务回到 `retrying`。
  实测：杀掉 Worker 后 2.9 秒被回收，`status=retrying last_error="worker lease expired"`，
  `jobraft_tasks_lease_expired_total` +1，另一个 Worker 以 `attempts=2` 接手并成功
  （见 [`demo.md`](demo.md) 第 3 幕）。
- **怎么保证不重复执行？** 保证不了；保证的是重复是可见的、可重试的，并且 handler 幂等。
  完成路径上的 `lease_token` 条件更新确保只有当前持有者能写状态。
- **优先级会饿死低优先级任务吗？** 会。`priority DESC` 只影响认领顺序，没有老化机制。
  这是已知边界，写了不假装。
- **依赖环怎么处理？** 提交时遍历依赖图，发现环直接拒绝
  （`dependency cycle detected through %s`）；运行期某个依赖失败，后继任务被判 `failed`
  并带上 `dependency %s did not succeed`。
- **PostgreSQL 是瓶颈吗？** 在这个规模不是。逐操作成本表在
  [`benchmark-results.md`](benchmark-results.md)：10,000 行积压时认领 6.98 ms、
  心跳+续租 4.87 ms、创建 3.84 ms；端到端只有 8 个 Worker、两个实例，数据库远没饱和。
  更诚实的说法是：端到端的 run-to-run 方差（±10–20%）比 store 优化的幅度更大，
  所以我没有继续"优化数字"，而是把测量工具和解释留在仓库里。
- **你修过的最有意思的 bug？** 两个 API 实例各自用 `task-<纳秒>-<计数器>` 生成 ID，
  共享一张表时同一时钟 tick 会撞主键；而主键冲突原先被映射成"任务在你读它之后被改了"
  （`ErrConflict`），客户端看到 400，真正的错因却是 ID 生成不唯一。修法：`internal/task/id.go`
  给每个进程一个随机 tag（`task-<纳秒>-<tag>-<seq>`），并把主键冲突单独映射为
  `ErrDuplicateID` → HTTP 409。
- **CI 里的 flake 怎么查的？** `TestPostgresClaimDueIsAtomic` 假设两个并发认领必须各拿一个任务，
  但候选集可能被一个事务整段锁走（`SKIP LOCKED` 于是返回 `ErrNoTaskAvailable`）。
  我先写了个探针量了一下：两个到期任务 + 两个并发认领，50 轮里有 49 轮出现一次"空手而归"。
  于是把测试改成"重试到两个任务都被认领，但绝不允许同一个任务被发两次"，并把 CI 的失败注解
  改成能打印断言行（否则注解只点名测试、看不出断言）。
- **压测数字为什么波动？** 见 [`benchmark-results.md`](benchmark-results.md)：同机 15 次运行的
  中位数落在 129–156 tasks/s（总中位 137、区间 118–167），所以简历里给的是中位数与范围，
  不是最好那一次。

## 如果重做会怎样

- 把每个任务约 7 次 store 往返压到 2–3 次：心跳与续租合并、完成时顺带取下一个任务。
- 认领一次交付一批（事务已经锁了 64 行，却只返回一条）。
- 先用 pprof + `pg_stat_statements` 找瓶颈，再动手改；这次是先猜后测，测出来才发现主因
  在压测工具自己（把清理时间算进了测量窗口、连接不复用导致临时端口耗尽）。
- 把 `/admin` 看板与调度器二进制解耦。

## 简历用（英文，可整段粘贴）

```
JobRaft (Go, PostgreSQL, Docker) — multi-instance task scheduler
- Built delayed, dependency-gated, and recurring task scheduling with at-least-once
  delivery: Worker leases, lease-token compare-and-set completion, retry budgets with
  backoff, idempotent submission, cancellation, and a documented task state machine.
- Implemented atomic cross-instance claiming in PostgreSQL: one transaction using
  FOR UPDATE SKIP LOCKED, a partial index matching the priority order (claim on a
  10,000-row backlog: 34.08 ms -> 6.98 ms; EXPLAIN 15.557 ms -> 0.109 ms), and
  index-backed scheduler ticks that read only due work and lapsed leases
  (about 136 ms -> 5 ms per tick on a 10,000-row table).
- Added a containerised two-instance deployment with Prometheus/Grafana, PostgreSQL
  integration tests, race-test CI, and a benchmark harness: 1,000 tasks across 8
  external Workers at a median of 131 tasks/s with ~3 s mean queue latency.
```

## 面试前 10 分钟自检

```bash
gofmt -l .                       # 应为空
go vet ./...                     # 应为空
JOBRAFT_TEST_DATABASE_URL=postgres://jobraft:jobraft@localhost:15432/jobraft?sslmode=disable \
  go test ./... -count=1         # 全绿（含 Postgres 集成测试）
docker compose up --build -d     # 或改走 demo.md 的 B 方案
```

然后按 [`demo.md`](demo.md) 把三幕跑一遍：依赖闸门、杀掉 Worker 看租约恢复、杀掉一个实例任务照常完成。
