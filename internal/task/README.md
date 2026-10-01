# internal/task

被其他所有包共享的领域模型。它定义了 `Task` 结构体、任务生命周期（`Status`）、`RetryPolicy`，以及调度器与存储所依赖的校验规则。

## 内容

- `Status` —— 六种生命周期状态：`pending`、`running`、`success`、`retrying`、`failed`、`canceled`。
- `Task` —— 一个工作单元，包含幂等键、优先级、依赖、负载、结果、重试策略、周期、超时、租约字段与时间戳。
- `RetryPolicy` —— `max_attempts` 与 `backoff`。
- `Validate` —— 强制调度器与存储依赖的必填字段与非负时长。
- `IsTerminal` —— 报告是否不再期望进一步的状态迁移。

该包是 `internal` 的，因此只在模块内部消费。它的 JSON 标签就是 HTTP API 服务的线上格式，因而是客户端负载的权威来源。
