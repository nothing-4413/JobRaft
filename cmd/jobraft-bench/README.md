# cmd/jobraft-bench

一个零依赖的负载生成器，通过一个或多个 JobRaft API 提交任务，再以外部 Worker 身份认领并完成它们。它刻意自包含（纯 `net/http`），使结果可复现。

## 用法

```bash
go run ./cmd/jobraft-bench -tasks 1000 -workers 8 -submit-parallelism 16
```

### 参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-endpoints` | `http://localhost:8080,http://localhost:8081` | 逗号分隔的 API URL。 |
| `-token` | `local-dev-token` | 每次请求携带的 bearer token。 |
| `-tasks` | `1000` | 要提交的任务数。 |
| `-workers` | `8` | 并发外部 Worker 数。 |
| `-submit-parallelism` | `16` | 并发提交者数。 |
| `-timeout` | `2m` | 端到端超时。 |

## 流程

1. 清理各端点上残留的 `benchmark` 任务；这段耗时单独记为 `cleanup_duration`，不计入测量窗口。
2. 按轮询顺序跨所有端点提交任务（name 为 `benchmark`）。
3. 运行外部 Worker，跨端点完成 register、heartbeat、长轮询 claim，然后 complete。
4. 打印 `cleanup_duration`、提交/耗时、吞吐，以及每个端点的 `/metrics`。

## 测量口径

- `submit_duration` 与 `elapsed`（吞吐的分母）只覆盖提交与执行阶段。清理代价只取决于上一次运行残留的行数（每行约 4 ms），与本次运行无关；把它算进去会让一个从非空队列起步的运行看起来慢好几倍。
- 客户端复用 HTTP 连接（`MaxIdleConnsPerHost = 256`）。Go 默认每个主机只保留 2 个空闲连接，数千次请求会不断关闭连接并把宿主机临时端口堆满 `TIME_WAIT`：这既抬高延迟，也会让后续运行以 `only one usage of each socket address` 直接失败。

参考运行记录及复现方法见 [`docs/benchmark-results.md`](../../docs/benchmark-results.md)。
