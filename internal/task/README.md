# internal/task

The domain model shared by every other package. It defines the `Task` struct, the
task lifecycle (`Status`), the `RetryPolicy`, and the validation rules the
scheduler and stores rely on.

## Contents

- `Status` — the six lifecycle states: `pending`, `running`, `success`,
  `retrying`, `failed`, and `canceled`.
- `Task` — a unit of work with idempotency key, priority, dependencies, payload,
  result, retry policy, schedule, timeout, lease fields, and timestamps.
- `RetryPolicy` — `max_attempts` and `backoff`.
- `Validate` — enforces the required fields and non-negative durations.
- `IsTerminal` — reports whether no further state transitions are expected.

This package is `internal`, so it is consumed only inside the module. Its JSON
tags are the wire format served by the HTTP API and therefore the source of truth
for client payloads.
