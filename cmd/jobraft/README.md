# cmd/jobraft

JobRaft 服务端二进制。它把 [`store`](../../internal/store)、[`scheduler`](../../internal/scheduler)、[`api`](../../internal/api) 服务器以及（配置时）[`cluster`](../../internal/cluster) 选主器装配在一起。

## 运行

```bash
go run ./cmd/jobraft
```

服务默认监听 `:8080`。完整的环境变量参考与示例请求见根目录 [`README.md`](../../README.md)。

## 启动行为

- 存储默认使用内存实现；设置 `JOBRAFT_STORE` 使用单进程 JSON 持久化，或设置 `JOBRAFT_DATABASE_URL` 使用 PostgreSQL（后者优先，并启用多实例原子认领）。
- `JOBRAFT_WORKERS` 控制多少个本地处理 Worker 分发已注册处理函数的任务（二进制内置注册了一个空操作的 `echo` 处理器）。
- `JOBRAFT_NODE_ID` 开启领导选举；`JOBRAFT_CLUSTER_FILE` 用共享锁文件替代内存注册表。
- `JOBRAFT_API_TOKEN` 在除 `/healthz` 与 `/readyz` 之外的所有端点启用 bearer/API key 鉴权。

## 优雅停机

进程捕获 `SIGINT`/`SIGTERM`，停止调度器，并以五秒超时排空 HTTP 服务器。仍在本地 Worker 上运行的任务会被重新入队为可重试任务。
