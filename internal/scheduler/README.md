# internal/scheduler

执行引擎。它负责任务分发、租约、重试、取消、依赖、背压与指标。

## 要点

- `Register(name, handler)` 安装进程内处理函数；`Submit` 校验依赖（含环检测）与幂等键。
- 一个基于 ticker 的 `dispatch` 循环把到期任务送入有界队列，由本地 Worker 消费；没有本地处理函数的任务保持 pending，等待外部 Worker 认领。
- `Claim`、`CompleteTaskWithResult`、`RenewTaskLease` 通过租约 token 服务外部 Worker，并通过条件存储更新强制执行。
- 过期租约会被回收为 `retrying`；重启时会恢复在途任务。
- `SetMaxPending` 施加背压（API 层返回 HTTP 429），`SetLeaseTTL` 调整租约时长。
- 开启选举时，`SetLeaderGate` 让非领导节点暂停分发与认领。
- `Metrics` 与 `QueueLatencyHistogram` 驱动 `/metrics` 端点。

投递是至少一次（at-least-once）的，因此处理函数必须幂等。完整语义见 [`docs/architecture.md`](../../docs/architecture.md)。
