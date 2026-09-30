# cmd/jobraft-bench

A dependency-free load generator that submits tasks through one or more JobRaft
APIs, then claims and completes them as external workers. It is intentionally
self-contained (plain `net/http`) so results are reproducible.

## Usage

```bash
go run ./cmd/jobraft-bench -tasks 1000 -workers 8 -submit-parallelism 16
```

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-endpoints` | `http://localhost:8080,http://localhost:8081` | Comma-separated API URLs. |
| `-token` | `local-dev-token` | Bearer token sent on each request. |
| `-tasks` | `1000` | Number of tasks to submit. |
| `-workers` | `8` | Concurrent external workers. |
| `-submit-parallelism` | `16` | Concurrent submitters. |
| `-timeout` | `2m` | End-to-end timeout. |

## Flow

1. Cleans up any leftover `benchmark` tasks on the first endpoint.
2. Submits tasks (name `benchmark`) across all endpoints in round-robin order.
3. Runs external workers that register, heartbeat, long-poll claim, then
   complete each task across the endpoints.
4. Prints submission/elapsed time, throughput, and each endpoint's `/metrics`.

See [`docs/benchmark-results.md`](../../docs/benchmark-results.md) for a recorded
reference run and how to reproduce it.
