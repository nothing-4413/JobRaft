# Benchmark Results

This file records measurements produced by a command shown below, including
machine details and exact task/Worker counts. Older rows are kept as history:
read a row's notes before comparing it with a newer one.

## Reproduction

### A. Docker Compose (both API instances in containers)

```powershell
docker compose up --build -d
go run ./cmd/jobraft-bench -tasks 1000 -workers 8 -submit-parallelism 16
```

The benchmark submits tasks named `benchmark`, which are intentionally handled
by its external Workers instead of the binary's local `echo` handler. Submits,
claims, and completions alternate between `http://localhost:8080` and
`http://localhost:8081`. The Compose profile sets `JOBRAFT_MAX_PENDING=10000`
for the 1,000-task sample while retaining the binary's protective default.

### B. Host-native API instances (used for the 2026-10-07 row)

Building the image needs `golang:1.22-alpine` and `alpine:3.20` from Docker Hub;
when the registry is unreachable the two API instances can run straight from the
checkout and still share the Compose PostgreSQL. Publish Postgres to the host
first, because `docker-compose.yml` maps no port for it:

```powershell
docker compose up -d postgres        # plus ports: ["15432:5432"] for that service,
                                     # e.g. through a small override file
$env:JOBRAFT_DATABASE_URL = 'postgres://jobraft:jobraft@localhost:15432/jobraft?sslmode=disable'
$env:JOBRAFT_API_TOKEN    = 'local-dev-token'
$env:JOBRAFT_MAX_PENDING  = '10000'
$env:JOBRAFT_LEASE_TTL    = '3s'
$env:JOBRAFT_ADDR = ':18080'; go run ./cmd/jobraft      # first instance
$env:JOBRAFT_ADDR = ':18081'; go run ./cmd/jobraft      # second instance
go run ./cmd/jobraft-bench -endpoints http://localhost:18080,http://localhost:18081 `
  -tasks 1000 -workers 8 -submit-parallelism 16
```

Two properties of the harness matter for reading these numbers:

- **The measured window excludes the harness's own cleanup.** A run first
  deletes `benchmark` tasks left behind by an earlier run, which costs roughly
  4 ms per leftover row (4.7 s for 1,000). That time is reported separately as
  `cleanup_duration` and is not part of `submit_duration`, `elapsed`, or the
  throughput figure.
- **Connections are reused across the run.** With Go's default idle pool (two
  per host) a few thousand requests close nearly every connection and fill the
  host's ephemeral ports with `TIME_WAIT` entries. That inflates latency and,
  once the port range is full, fails the run with `only one usage of each socket
  address` before it starts.

## Record

| Date | Machine | Tasks | Workers | Submit parallelism | Setting | End-to-end time | Throughput | Queue latency |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: | ---: |
| 2026-09-23 | Lenovo 83DF, Intel i9-14900HX, 32 logical CPUs, 32 GB RAM; Docker Desktop + PostgreSQL 16 | 1,000 | 8 | 16 | Compose, two instances | 24.587 s | 40.67 tasks/s | P95 14.70 s |
| 2026-10-07 | Lenovo 83DF, Intel i9-14900HX, 32 logical CPUs, 32 GB RAM; Docker Desktop 29.7.2 + PostgreSQL 16-alpine | 1,000 | 8 | 16 | two host-native instances, fresh store, median of 5 runs | 7.75 s | 129.0 tasks/s | mean 3.0 s, P95 ≤ 10 s |

The 2026-10-07 row: five consecutive runs, each starting from an empty store.
Every run completed all 1,000 tasks with zero failed, retried, or expired
leases, 1,000 unique completion IDs and no duplicate completions. Throughput
ranged 118.2–139.4 tasks/s (mean 129.5); the submit phase took 1.10–1.48 s
(≈ 700–900 submissions/s with 16 submitters); end-to-end time ranged 7.17–8.46 s.
Mean queue latency ranged 2.88–3.52 s. Queue latency was read as the
`jobraft_task_queue_latency_seconds` histogram delta around each run, so its
precision is limited by the bucket bounds (5 s and 10 s); the 95th percentile
landed above 5 s in three runs and at or below 5 s in two. These are
development-laptop figures, not a capacity guarantee.

The 2026-09-23 row predates the harness fixes above: its total includes deleting
the previous run's `benchmark` rows, so it is not comparable with the newer row
and should not be quoted as the current throughput. It is retained because it is
what the project's earlier documentation reported.

Take P95 from the `Queue Latency P95` panel at `http://localhost:3000` using
`admin` / `local-dev-password`. Prometheus is available on port `9090`.

## Store operation cost

Where a task's time goes. `internal/store/postgres_cost_test.go` is opt-in
(`JOBRAFT_BENCH_CLAIM=1`) and times the store calls the scheduler makes per
task, at three queue depths, plus a claim of a task with five dependencies:

```powershell
$env:JOBRAFT_TEST_DATABASE_URL = 'postgres://jobraft:jobraft@localhost:15432/jobraft?sslmode=disable'
$env:JOBRAFT_BENCH_CLAIM       = '1'
go test ./internal/store -run TestPostgresOperationCost -v -count=1
# inside the Compose network instead: bash scripts/store-cost.sh
```

| Operation (mean) | depth 0 | depth 1,000 | depth 10,000 |
| --- | ---: | ---: | ---: |
| `list` (20 samples) | 0.42 ms | 3.64 ms | 49.83 ms |
| `count-in-flight` | 0.36 ms | 0.61 ms | 1.24 ms |
| `create` | 3.37 ms | 3.22 ms | 3.36 ms |
| `heartbeat + renew` | 4.23 ms | 3.87 ms | 4.61 ms |
| `claim` (`FOR UPDATE SKIP LOCKED`) | 5.80 ms | 7.74 ms | 34.08 ms |
| `complete` | – | 12.34 ms | 42.16 ms |
| `claim` with 5 dependencies | – | 14.20 ms | – |

Measured on the same machine from the Windows host against the published
Postgres port, so every round trip carries the port-forward cost and these are
upper bounds for an in-network run. `claim` and `complete` grow with the queue
depth because a claim locks up to 64 candidate rows and returns one, and because
the dependency check issues one `FOR KEY SHARE` query per dependency
(`internal/store/postgres.go`). `create` and the lease heartbeat stay flat.
`complete` is the `Get` plus the lease-guarded `Update` a Worker's completion
performs.

A scheduler tick reads two things: the work that is due, and the running tasks
whose lease has lapsed. Taking both from a full listing means paying the `list`
row above twice per tick. `ListDue` and `ListExpired` answer the same two
questions with an index lookup, measured on a 10,000-row table shaped like a
deployment that has been up for a while — 9,000 finished tasks, 500 due, 500
running under a valid lease, 50 lapsed:

| Operation (mean) | 10,000-row table |
| --- | ---: |
| `list` (every row) | 48.97 ms |
| `list-due` (500 due) | 4.04 ms |
| `list-expired` (50 lapsed) | 1.27 ms |

That is about 98 ms of reads per tick down to about 5 ms, and the smaller number
is the one that stops growing when the table does. Scheduled work is unaffected:
a tick still considers every task that has arrived and every lease that lapsed,
because a limit on either scan could starve a task behind a long run of work
that is not ready yet.
