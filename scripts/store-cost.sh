#!/usr/bin/env bash
# Measure what one task costs the PostgreSQL store at different queue depths.
#
# The reference runs in docs/benchmark-results.md are dominated by these
# per-statement costs, and a cost that grows with the queue is invisible until
# the queue is deep. The mixed-state subtest also times the two reads a
# scheduler tick performs, against a table that is mostly finished work.
#
#   docker compose up -d postgres
#   scripts/store-cost.sh
#
# Override with DEPTHS (a Go subtest regex) or GO_IMAGE:
#
#   DEPTHS='depth10000|dependencies' scripts/store-cost.sh

set -euo pipefail

GO_IMAGE="${GO_IMAGE:-golang:1.22-bookworm}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_NETWORK="${COMPOSE_NETWORK:-jobraft_default}"
DEPTHS="${DEPTHS:-.}"
DATABASE_URL="${JOBRAFT_TEST_DATABASE_URL:-postgres://jobraft:jobraft@postgres:5432/jobraft?sslmode=disable}"

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
  -e JOBRAFT_TEST_DATABASE_URL="$DATABASE_URL" \
  -e JOBRAFT_BENCH_CLAIM=1 \
  "$GO_IMAGE" \
  go test ./internal/store -run "TestPostgresOperationCost/$DEPTHS" -v -count=1
