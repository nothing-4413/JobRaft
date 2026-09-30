# cmd/jobraft

The JobRaft server binary. It wires together a [`store`](../../internal/store),
a [`scheduler`](../../internal/scheduler), an [`api`](../../internal/api) server,
and — when configured — a [`cluster`](../../internal/cluster) elector.

## Run

```bash
go run ./cmd/jobraft
```

The server listens on `:8080` by default. See the root
[`README.md`](../../README.md) for the full environment-variable reference and
example requests.

## Startup behavior

- Storage defaults to the in-memory store; set `JOBRAFT_STORE` for single-process
  JSON persistence or `JOBRAFT_DATABASE_URL` for PostgreSQL (which takes
  precedence and enables multi-instance atomic claims).
- `JOBRAFT_WORKERS` controls how many local handler workers dispatch tasks that
  have a registered handler (the binary registers a no-op `echo` handler).
- `JOBRAFT_NODE_ID` enables leader election; `JOBRAFT_CLUSTER_FILE` backs it with
  a shared lock file instead of the in-memory registry.
- `JOBRAFT_API_TOKEN` enables bearer/API-key authentication on every endpoint
  except `/healthz` and `/readyz`.

## Graceful shutdown

The process traps `SIGINT`/`SIGTERM`, stops the scheduler, and drains the HTTP
server with a five-second timeout. Tasks still running on local workers are
requeued as retryable work.
