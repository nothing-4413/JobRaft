# internal/scheduler

The execution engine. It owns work dispatch, leases, retries, cancellation,
dependencies, backpressure, and metrics.

## Highlights

- `Register(name, handler)` installs in-process handlers; `Submit` validates
  dependencies (including cycle detection) and idempotency keys.
- A ticker-driven `dispatch` loop moves due tasks into a bounded queue consumed
  by local workers; tasks without a local handler stay pending for external
  workers to claim.
- `Claim`, `CompleteTaskWithResult`, and `RenewTaskLease` serve external workers
  with lease tokens, enforced through conditional store updates.
- Expired leases are reaped back to `retrying`; in-flight work is recovered on
  restart.
- `SetMaxPending` applies backpressure (HTTP 429 at the API layer) and
  `SetLeaseTTL` tunes the lease duration.
- `SetLeaderGate` pauses dispatch and claims on non-leader nodes when election is
  enabled.
- `Metrics` and `QueueLatencyHistogram` feed the `/metrics` endpoint.

Delivery is at-least-once, so handlers must be idempotent. See
[`docs/architecture.md`](../../docs/architecture.md) for the full semantics.
