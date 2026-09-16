# JobRaft

JobRaft is a small Go workflow/task scheduler built in stages. The first stage
contains an embeddable scheduler and an HTTP API with delayed execution,
timeouts, cancellation, retries, and task status inspection.

## Run

```bash
go run ./cmd/jobraft
```

The server listens on `:8080`.

Create a task:

```bash
curl -X POST http://localhost:8080/tasks \
  -H 'content-type: application/json' \
  -d '{"name":"echo","payload":{"message":"hello"},"delay": 0, "retry":{"max_attempts":3}}'
```

Inspect, list, or cancel tasks with `GET /tasks`, `GET /tasks/{id}`, and
`DELETE /tasks/{id}`. Set a larger `priority` value to run eligible work first.
Tasks can declare predecessor IDs through `depends_on`; they run only after all
predecessors succeed, and fail automatically if a predecessor fails or is
canceled.
Set `schedule` to a duration in nanoseconds to create a recurring task; after
each successful run it returns to `pending` and is scheduled again.

Workers can be registered and kept alive with `POST /workers` and
`POST /workers/{id}`; inspect them with `GET /workers`.
An external worker can pull work with `POST /workers/{id}/claim` and acknowledge
it using `POST /tasks/{id}?complete=true` with `worker_id`, `lease_token`, and
an optional `error` field. Heartbeats renew the worker's active task leases.
Successful workers may also send a JSON `result`; it is stored on the task and
returned by the task query/list APIs. The SDK exposes this as
`CompleteWithResult`.
Add `?wait=10s` to the claim request for long polling (maximum 30 seconds).
Prometheus-compatible counters are available from `GET /metrics`.
The endpoint also exposes current pending, running, retrying, and online-worker
gauges for queue pressure dashboards.
Use `/healthz` for liveness and `/readyz` for readiness probes.
The scheduler applies a configurable in-flight task limit through
`SetMaxPending`; submissions over the limit receive HTTP 429.

For leader-election mode, set `JOBRAFT_NODE_ID`. The in-memory election
registry exposes the current node/leader through `GET /cluster`; the registry
interface is designed to be replaced by a Raft-backed implementation for
multi-process deployments.
With `JOBRAFT_CLUSTER_FILE`, a shared JSON file and exclusive lock provide a
lightweight cross-process lease election. Use a consensus-backed registry for
network partitions and larger clusters.

`internal/cluster.ConsensusGroup` provides an embedded replicated-log
state-machine and quorum API for deterministic tests and local integration.
It intentionally does not claim to replace a full Raft implementation across
untrusted networks; the `ReplicatedLog` interface is the boundary for adding
that transport later.

External workers can use `pkg/workerclient`'s `Client.Run` to handle the
register/heartbeat/claim/complete loop without manually constructing HTTP
requests.
Open `GET /admin` in a browser for a lightweight live management dashboard.

Deployment settings:

- `JOBRAFT_ADDR` (default `:8080`)
- `JOBRAFT_WORKERS` (default `4`)
- `JOBRAFT_STORE` (optional JSON persistence path)
- `JOBRAFT_NODE_ID` (optional leader-election identity)
- `JOBRAFT_CLUSTER_FILE` (optional shared registry file for multi-process leader election)
- `JOBRAFT_MAX_PENDING` (optional in-flight task limit)
- `JOBRAFT_LEASE_TTL` (optional worker/task lease duration, e.g. `30s`)

Build a container with `docker build -t jobraft .` and run it with a writable
`/data` volume for persistence.

GitHub Actions runs formatting, tests, and a full build on every push and pull
request.

## State machine

Tasks move from `pending` to `running`, then to `success`. A failed execution
becomes `retrying` until its retry budget is exhausted, after which it is
`failed`. Cancellation is terminal. Delivery is intentionally at-least-once;
handlers should be idempotent.

The storage layer is an interface (`internal/store.Store`) so SQLite/PostgreSQL
backends can be added without changing scheduler logic. The default binary uses
the concurrency-safe in-memory implementation.

When `JOBRAFT_STORE` points to a JSON file, tasks that were running when the
process stopped are recovered as retryable work on the next startup.
