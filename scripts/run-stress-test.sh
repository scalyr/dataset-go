#!/usr/bin/env bash
#
# Builds examples/stress and runs it against an in-process fake DataSet server.
#
# Usage: scripts/run-stress-test.sh [EVENTS] [BUCKETS]
#   EVENTS  - positive integer (default 10000)
#   BUCKETS - positive integer, or -1 for maximum congestion (default 10000)
#
# Exits with the stress binary's status. pipefail matters here: without it the
# status of `| tee` would hide a failed run.

set -euo pipefail

# `-` not `:-`: an explicitly empty argument is an error, not the default.
EVENTS="${1-10000}"
BUCKETS="${2-10000}"

usage() {
  sed -n 's/^# \{0,1\}//; 5,7p' "$0" >&2
  exit 2
}
[[ $# -le 2 ]] || usage
[[ "${EVENTS}" =~ ^[1-9][0-9]*$ ]] || usage
[[ "${BUCKETS}" =~ ^([1-9][0-9]*|-1)$ ]] || usage

cd "$(dirname "$0")/../examples/stress"

rm -fv stress
go build -race -o stress
ls -l stress

echo "Run stress test for ${EVENTS} events and ${BUCKETS} buckets"
./stress \
  --events="${EVENTS}" \
  --buckets="${BUCKETS}" \
  --sleep=10ms 2>&1 | tee "out-${EVENTS}-${BUCKETS}.log"
