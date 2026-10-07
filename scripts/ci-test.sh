#!/usr/bin/env bash
# Run a Go test command and re-emit its failures as GitHub annotations.
#
#   scripts/ci-test.sh go test -race ./...
#
# Job logs need admin rights even on a public repository, so a red CI run is
# otherwise invisible from the outside. Annotations are part of the check run
# and can be read without a token, which is what makes a failure diagnosable.

set -uo pipefail

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

"$@" 2>&1 | tee "$log"
status=$?

if [ "$status" -ne 0 ]; then
  # A failing test prints "--- FAIL: TestX" and then an indented
  # "<file>_test.go:<line>: <message>" line. Without that second line the
  # annotation names the test but not the assertion, which leaves the failure
  # half-diagnosed.
  grep -nE '^(--- FAIL|FAIL|ok  |panic:|fatal error:|WARNING: DATA RACE)|_test\.go:[0-9]+:' "$log" | head -12 |
    while IFS= read -r line; do
      echo "::error title=go test failure::$line"
    done
fi

exit "$status"
