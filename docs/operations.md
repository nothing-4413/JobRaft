# Operations and Failure Semantics

## Local multi-instance deployment

`docker compose up --build` starts PostgreSQL and two JobRaft API instances.
The public ports are `8080` and `8081`; both instances share the PostgreSQL task store.
Use `JOBRAFT_API_TOKEN=local-dev-token` for local API requests.
Prometheus is exposed on `9090` and Grafana on `3000` with the local-only
credentials `admin` / `local-dev-password`.

The PostgreSQL store creates its schema on startup. `JOBRAFT_DATABASE_URL` takes
precedence over `JOBRAFT_STORE`. The JSON store remains useful for a single
process or an offline demo, but it must not be shared by multiple processes.
This compose example deliberately does not enable the file-based elector:
containers do not share a local filesystem, and PostgreSQL is the shared
coordination boundary for task claims.
It sets a three-second lease TTL only to make local recovery demonstrations
quick; use a longer TTL appropriate to production task duration.

## Delivery and recovery

Task delivery is at-least-once. A worker claim changes a task to `running` and
assigns a lease token inside a transaction. Competing instances use
`FOR UPDATE SKIP LOCKED`, so only one transaction can claim a row. If a worker
dies or stops renewing the lease, the scheduler moves the task to `retrying`.
The task can then be claimed again; handlers must therefore be idempotent.

Completion and cancellation use the task's worker and lease token as a
compare-and-set condition. A stale worker cannot overwrite a newer retry or a
cancellation.

## Verification checklist

- `go test ./...`
- `go test -race ./...`
- `docker compose up --build`
- `go run ./cmd/jobraft-bench -tasks 1000 -workers 8`
- Submit tasks through one instance and claim them through the other.
- Stop a worker process while a task is running and verify it becomes
  `retrying` after the lease expires.
- Retry the same submission with the same `Idempotency-Key` and verify one task
  is returned.

The PostgreSQL tests run when `JOBRAFT_TEST_DATABASE_URL` is set; otherwise they
skip so unit tests remain usable without external services. CI starts a
PostgreSQL service and sets this variable automatically.

For production, use a managed PostgreSQL service, a strong API token, TLS for
the database connection, and an external consensus/lease system if leader
election must survive a shared filesystem failure.
