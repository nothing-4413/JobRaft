# Benchmark Results

This file is intentionally a template. Record only measurements produced by a
command shown below, including machine details and exact task/Worker counts.

## Reproduction

```powershell
docker compose up --build -d
go run ./cmd/jobraft-bench -tasks 1000 -workers 8 -submit-parallelism 16
```

The benchmark submits tasks named `benchmark`, which are intentionally handled
by its external Workers instead of the binary's local `echo` handler. Submits,
claims, and completions alternate between `http://localhost:8080` and
`http://localhost:8081`. The Compose profile sets `JOBRAFT_MAX_PENDING=10000`
for the 1,000-task sample while retaining the binary's protective default.

## Record

| Date | Machine | Tasks | Workers | Submit parallelism | End-to-end time | Throughput | P95 queue latency |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 2026-09-23 | Lenovo 83DF, Intel i9-14900HX, 32 logical CPUs, 32 GB RAM; Docker Desktop + PostgreSQL 16 | 1,000 | 8 | 16 | 24.587 s | 40.67 tasks/s | 14.70 s |

The run completed all 1,000 tasks with zero failed, retried, or expired leases.
The two API instances accepted 507/493 submissions and 500/500 completions,
respectively. P95 was queried from Prometheus after the run using the supplied
histogram query; it should be treated as a development-machine reference, not a
production capacity guarantee.

Take P95 from the `Queue Latency P95` panel at `http://localhost:3000` using
`admin` / `local-dev-password`. Prometheus is available on port `9090`.
