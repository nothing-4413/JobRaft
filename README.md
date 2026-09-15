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

Inspect or cancel it with `GET /tasks/{id}` and `DELETE /tasks/{id}`.

## State machine

Tasks move from `pending` to `running`, then to `success`. A failed execution
becomes `retrying` until its retry budget is exhausted, after which it is
`failed`. Cancellation is terminal. Delivery is intentionally at-least-once;
handlers should be idempotent.

The storage layer is an interface (`internal/store.Store`) so SQLite/PostgreSQL
backends can be added without changing scheduler logic. The default binary uses
the concurrency-safe in-memory implementation.
