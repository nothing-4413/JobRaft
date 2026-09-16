# JobRaft HTTP API

All request and response bodies use JSON unless noted otherwise.

## Tasks

- `POST /tasks`: create a task. Fields include `name`, `payload`, `delay` or
  `run_at`, `timeout`, `schedule`, `priority`, `depends_on`, and `retry`.
- `GET /tasks`: list tasks.
- `GET /tasks/{id}`: inspect a task and its optional `result`.
- `DELETE /tasks/{id}`: cancel a task.
- `POST /tasks/{id}?complete=true`: complete a claimed task with
  `worker_id`, `lease_token`, optional `error`, and optional `result`.

## Workers

- `POST /workers`: register `{ "id": "worker-1" }`.
- `POST /workers/{id}`: heartbeat and renew leases.
- `POST /workers/{id}/claim?wait=10s`: claim a task, optionally using long poll.
- `GET /workers`: list workers.

## Operations

- `GET /healthz`: liveness.
- `GET /readyz`: readiness.
- `GET /metrics`: Prometheus text format.
- `GET /cluster`: leader and node information when cluster election is enabled.
- `GET /admin`: live browser dashboard.

