#!/usr/bin/env bash
# Run exactly what .github/workflows/ci.yml runs, in the same order.
#
# Use it before pushing so a failure is caught locally instead of in CI.
# The PostgreSQL tests are skipped when JOBRAFT_TEST_DATABASE_URL is unset,
# so the first three steps work with nothing but a Go toolchain installed.
#
#   ./scripts/ci-check.sh
#   JOBRAFT_TEST_DATABASE_URL='postgres://jobraft:jobraft@localhost:5432/jobraft?sslmode=disable' \
#     ./scripts/ci-check.sh
#
# Inside Docker:
#   docker run --rm -v "$PWD:/src" -w /src -e GOCACHE=/tmp/gocache \
#     golang:1.22-bookworm bash scripts/ci-check.sh

set -euo pipefail

step() { printf '\n=== %s ===\n' "$1"; }

step "go version"
go version

step "gofmt"
unformatted="$(gofmt -l .)"
if [ -n "$unformatted" ]; then
  printf 'the following files are not gofmt-clean:\n%s\n' "$unformatted" >&2
  exit 1
fi
echo "gofmt clean"

step "go vet"
go vet ./...

step "go build"
go build ./...

step "go test"
go test ./...

step "go test -race"
go test -race ./...

printf '\nall checks passed\n'
