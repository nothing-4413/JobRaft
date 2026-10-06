#!/usr/bin/env bash
# Reproduce docs/benchmark-results.md against a local Docker Compose stack.
#
#   docker compose up --build -d      # two API instances + PostgreSQL
#   scripts/benchmark.sh              # 1,000 tasks, 8 workers, 16 submitters
#
# Runs the benchmark inside a golang container so only Docker is required on
# the host. Override the image or arguments via environment variables:
#
#   BENCH_ARGS='-tasks 2000 -workers 16' GO_IMAGE=golang:1.22-bookworm scripts/benchmark.sh

set -euo pipefail

GO_IMAGE="${GO_IMAGE:-golang:1.22-bookworm}"
BENCH_ARGS="${BENCH_ARGS:--tasks 1000 -workers 8 -submit-parallelism 16}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_NETWORK="${COMPOSE_NETWORK:-jobraft_default}"

if ! docker compose ps --status running --services 2>/dev/null | grep -qx postgres; then
  echo "the Compose stack is not running; start it with 'docker compose up --build -d'" >&2
  exit 1
fi

docker run --rm \
  --network "$COMPOSE_NETWORK" \
  -v "$REPO_DIR:/src" -w /src \
  -v jobraft-gocache:/tmp/gocache \
  -v jobraft-gomodcache:/gomodcache \
  -e GOCACHE=/tmp/gocache -e GOMODCACHE=/gomodcache \
  "$GO_IMAGE" \
  go run ./cmd/jobraft-bench $BENCH_ARGS
