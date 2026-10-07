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

### B. Host-native API instances (used for both 2026-10-07 rows)

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
  address` before it starts. On Windows the same exhaustion also hits the API
  instances' own PostgreSQL connections, which then surface inside API responses
  as a `dial tcp [::1]:15432: connectex: Only one usage of each socket address`
  body on a 404 or 409. Failed attempts leave rows behind, so a retried sample
  reports an inflated latency counter.

## Record

| Date | Machine | Tasks | Workers | Submit parallelism | Setting | End-to-end time | Throughput | Queue latency |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: | ---: |
| 2026-09-23 | Lenovo 83DF, Intel i9-14900HX, 32 logical CPUs, 32 GB RAM; Docker Desktop + PostgreSQL 16 | 1,000 | 8 | 16 | Compose, two instances | 24.587 s | 40.67 tasks/s | P95 14.70 s |
| 2026-10-07 | Lenovo 83DF, Intel i9-14900HX, 32 logical CPUs, 32 GB RAM; Docker Desktop 29.7.2 + PostgreSQL 16-alpine | 1,000 | 8 | 16 | two host-native instances, fresh store, median of 5 runs | 7.75 s | 129.0 tasks/s | mean 3.0 s, P95 ≤ 10 s |
| 2026-10-07 | same machine, after the store work (`ListDue` scans, claim index, batched dependency check) | 1,000 | 8 | 16 | two host-native instances, fresh store, median of 5 runs | 7.63 s | 131.1 tasks/s | mean 3.1 s, P95 ≤ 10 s |

The first 2026-10-07 row: five consecutive runs, each starting from an empty store.
Every run completed all 1,000 tasks with zero failed, retried, or expired
leases, 1,000 unique completion IDs and no duplicate completions. Throughput
ranged 118.2–139.4 tasks/s (mean 129.5); the submit phase took 1.10–1.48 s
(≈ 700–900 submissions/s with 16 submitters); end-to-end time ranged 7.17–8.46 s.
Mean queue latency ranged 2.88–3.52 s. Queue latency was read as the
`jobraft_task_queue_latency_seconds` histogram delta around each run, so its
precision is limited by the bucket bounds (5 s and 10 s); the 95th percentile
landed above 5 s in three runs and at or below 5 s in two. These are
development-laptop figures, not a capacity guarantee.

The third row repeats that measurement after the store-level work and lands in
the same place: throughput ranged 121.7–157.9 tasks/s (median 131.1, mean 136.0),
submit 0.97–1.59 s, end-to-end 6.33–8.22 s, mean queue latency 2.4–3.6 s, P95 at
or below 5 s in three runs and below 10 s in two. Across all fifteen runs on this
host the medians sit between 129 and 156 tasks/s (overall median 137, range
118–167), so run-to-run variance of roughly ±10–20 % is wider than the effect of
that work. The claim index and the targeted tick scans are visible in the store
table below (`claim` at depth 10,000 fell from 34.08 ms to 6.98 ms), not at this
granularity: the end-to-end run is dominated by HTTP round trips and lease
bookkeeping, and 8 Workers across two instances leave the database far from
saturated. Both batches are also honest about a host artifact: some samples needed
retries because the ephemeral port range was full, and those attempts left rows
behind (a retried sample reported 2,339 latency observations for 1,000 tasks).

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
| `list` (20 samples) | 0.51 ms | 20.19 ms¹ | 63.20 ms |
| `count-in-flight` | 0.45 ms | 0.49 ms | 1.84 ms |
| `create` | 3.68 ms | 3.71 ms | 3.84 ms |
| `heartbeat + renew` | 4.12 ms | 4.08 ms | 4.87 ms |
| `claim` (`FOR UPDATE SKIP LOCKED`) | 5.56 ms | 5.67 ms | 6.98 ms |
| `complete` (claim + complete) | – | 9.70 ms | 10.69 ms |
| `claim` with 5 dependencies | – | 10.41 ms | – |

¹ One of the 20 `list` samples at depth 1,000 took 308 ms, which pulls the mean
up; its p50 is 5.47 ms.

Measured in one run on the same machine from the Windows host against the
published Postgres port, so every round trip carries the port-forward cost and
these are upper bounds for an in-network run. `create` and the lease heartbeat
stay flat with depth. `complete` includes the `claim` that has to find the task
first, which is why it tracks the `claim` row.

### Why a claim does not get slower

A claim asks for the due tasks in priority order and stops as soon as it has its
batch of candidates, so it needs an index that is already in that order.
`jobraft_tasks_due_idx` leads with `status`, so the planner cannot use it for
`ORDER BY priority DESC, run_at, id` and sorted the entire due set on every
claim instead:

```
Limit  (actual time=15.420..15.452 rows=64)
  ->  LockRows
        ->  Sort  (Sort Key: priority DESC, run_at, id)
              Sort Method: quicksort  Memory: 1010kB
              ->  Seq Scan on jobraft_tasks  (rows=10000)
Execution Time: 15.557 ms
```

`jobraft_tasks_claim_idx` puts the claim order first and carries the claimable
statuses as its predicate, so the same query on the same 10,000-row backlog walks
the index and stops after 64 rows:

```
Limit  (actual time=0.036..0.084 rows=64)
  ->  LockRows
        ->  Index Scan using jobraft_tasks_claim_idx  (rows=64)
              Index Cond: (run_at <= now())
Execution Time: 0.109 ms
```

That is the difference between a claim that costs 34.08 ms on a 10,000-row
backlog and one that costs 6.98 ms, within noise of the 5.56 ms it costs on an
empty queue. Both plans come from `EXPLAIN (ANALYZE, BUFFERS)` on the same host.

The five-dependency claim locks the whole dependency set in a single
`FOR KEY SHARE` query. Checking one dependency at a time cost 14.20 ms for the
same task, so a task that waits on four other steps paid several extra round
trips on every claim attempt.

A scheduler tick reads two things: the work that is due, and the running tasks
whose lease has lapsed. Taking both from a full listing means paying the `list`
row above twice per tick. `ListDue` and `ListExpired` answer the same two
questions with an index lookup, measured on a 10,000-row table shaped like a
deployment that has been up for a while — 9,000 finished tasks, 500 due, 500
running under a valid lease, 50 lapsed:

| Operation (mean) | 10,000-row table |
| --- | ---: |
| `list` (every row) | 67.91 ms |
| `list-due` (500 due) | 3.54 ms |
| `list-expired` (50 lapsed) | 1.16 ms |

That is about 136 ms of reads per tick down to about 5 ms, and the smaller
number is the one that stops growing when the table does. Scheduled work is
unaffected: a tick still considers every task that has arrived and every lease
that lapsed, because a limit on either scan could starve a task behind a long run
of work that is not ready yet.
