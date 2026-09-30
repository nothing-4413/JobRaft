# internal/store

The persistence layer. It defines the `Store` contract plus three backends and
the worker-lease registry.

## Contracts

- `Store` — `Create`, `Get`, `List`, `Update`.
- `ConditionalUpdater` — compare-and-set updates keyed on lease token or status.
- `AtomicClaimer` — `ClaimDue`, used by shared backends to reserve work without
  the read/update race between scheduler processes.
- `WorkerRegistry` — register, heartbeat, list, and renew worker leases.

## Backends

| Type | Constructor | Use case |
| --- | --- | --- |
| `MemoryStore` | `NewMemory()` | Development, tests, single process. |
| `FileStore` | `NewFile(path)` | Single-process JSON persistence with restart recovery. |
| `PostgresStore` | `NewPostgres(ctx, dsn)` | Shared production store; `FOR UPDATE SKIP LOCKED` atomic claims. |

The file and PostgreSQL stores both implement the conditional-update and worker
interfaces. PostgreSQL creates its schema idempotently on startup. The `Due`
helper sorts eligible tasks by priority, then scheduled time.
