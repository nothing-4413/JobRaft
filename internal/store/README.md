# internal/store

持久化层。它定义 `Store` 契约、三种后端以及 Worker 租约注册表。

## 契约

- `Store` —— `Create`、`Get`、`List`、`Update`。
- `ConditionalUpdater` —— 以租约 token 或状态为条件的比较并交换（CAS）更新。
- `AtomicClaimer` —— `ClaimDue`，供共享后端用于在多个调度进程之间无读-改竞态地预留任务。
- `WorkerRegistry` —— 注册、心跳、列出与续期 Worker 租约。

## 后端

| 类型 | 构造函数 | 适用场景 |
| --- | --- | --- |
| `MemoryStore` | `NewMemory()` | 开发、测试、单进程。 |
| `FileStore` | `NewFile(path)` | 单进程 JSON 持久化，带重启恢复。 |
| `PostgresStore` | `NewPostgres(ctx, dsn)` | 共享生产存储；`FOR UPDATE SKIP LOCKED` 原子认领。 |

文件与 PostgreSQL 存储都实现了条件更新与 Worker 接口。PostgreSQL 在启动时幂等地创建 schema。`Due` 辅助函数按优先级、再按排期时间对符合条件的任务排序。
