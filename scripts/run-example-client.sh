#!/usr/bin/env bash
#
# End-to-end check against a real DataSet: runs examples/client, which sends
# its events tagged with a unique "param" attribute, then polls the query
# API (https://app.scalyr.com/help/api#query) until all of them can be read back.
#
# Usage: scripts/run-example-client.sh
#
# Environment (required):
#   SCALYR_SERVER          - DataSet URL, e.g. https://app.scalyr.com
#   SCALYR_WRITELOG_TOKEN  - token the client sends events with
#   SCALYR_READLOG_TOKEN   - token the query API is called with
# Environment (optional):
#   PARAM          - value of the "param" attribute (default: unique per run).
#                    Letters, digits, '.', '_' and '-' only: it goes into a query filter.
#   EXPECTED_EVENTS - events that must be found (default 10000, examples/client's BundleCount)
#   QUERY_TIMEOUT  - seconds to wait for all events to be queryable (default 300)
#   QUERY_INTERVAL - seconds between polls (default 10)
#
# Tokens are passed to curl through a private header file, never on its
# command line, and are never printed.

set -euo pipefail

die() {
  echo "run-example-client: $*" >&2
  exit 1
}

[[ $# -eq 0 ]] || {
  sed -n 's/^# \(Usage: .*\)/\1/p' "$0" >&2
  exit 2
}

# ============================================================================
# Phase 0: check the environment and settings
# ============================================================================

for var in SCALYR_SERVER SCALYR_WRITELOG_TOKEN SCALYR_READLOG_TOKEN; do
  [[ -n "${!var:-}" ]] || die "${var} is not set"
done
for tool in go curl jq; do
  command -v "${tool}" >/dev/null || die "${tool} is required"
done

PARAM="${PARAM:-dataset-go-e2e-$(date +%s)-${RANDOM}}"
EXPECTED_EVENTS="${EXPECTED_EVENTS:-10000}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-300}"
QUERY_INTERVAL="${QUERY_INTERVAL:-10}"
[[ "${PARAM}" =~ ^[A-Za-z0-9._-]+$ ]] || die "PARAM may contain only letters, digits, '.', '_' and '-'"
[[ "${EXPECTED_EVENTS}" =~ ^[1-9][0-9]*$ ]] || die "EXPECTED_EVENTS must be a positive integer"
[[ "${QUERY_TIMEOUT}" =~ ^[0-9]+$ ]] || die "QUERY_TIMEOUT must be a non-negative integer"
[[ "${QUERY_INTERVAL}" =~ ^[1-9][0-9]*$ ]] || die "QUERY_INTERVAL must be a positive integer"

server="${SCALYR_SERVER%/}"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ============================================================================
# Phase 1: build examples/client
# ============================================================================

echo "== Phase 1/4: building examples/client"
(
  cd "$(dirname "$0")/../examples/client"
  go build -o "${work}/client" .
)

# ============================================================================
# Phase 2: run the client, which sends the events
# ============================================================================

echo "== Phase 2/4: sending events to ${server} with param=${PARAM}"
# The client reads SCALYR_SERVER and SCALYR_WRITELOG_TOKEN itself.
"${work}/client" -param "${PARAM}" || die "examples/client failed"

# ============================================================================
# Phase 3: query API helpers (https://app.scalyr.com/help/api#query)
# ============================================================================

echo "== Phase 3/4: preparing queries to ${server}/api/query"
# The query API takes the token as a Bearer header, like addEvents.
(
  umask 077
  printf 'Authorization: Bearer %s\n' "${SCALYR_READLOG_TOKEN}" >"${work}/auth"
)

page_size=5000 # the query API's maximum maxCount
last_error=""  # why the latest poll failed transiently; empty after a good one

# query_page [CONTINUATION_TOKEN] : one query API call; the response is in
# ${work}/resp. Returns 0 on success, 1 on a transient failure worth retrying
# (no connection, HTTP 5xx), and dies on anything else (bad token, bad request).
query_page() {
  local body code
  body=$(jq -cn --arg filter "param == '${PARAM}'" --arg token "${1:-}" --argjson max "${page_size}" \
    '{queryType: "log", filter: $filter, startTime: "1h", maxCount: $max, priority: "low"}
     + (if $token == "" then {} else {continuationToken: $token} end)')
  code=$(curl -sS -o "${work}/resp" -w '%{http_code}' -X POST "${server}/api/query" \
    -H "Content-Type: application/json" -H @"${work}/auth" --data-binary "${body}") || code=000
  case "${code}" in
    200) ;;
    000 | 5??)
      last_error="HTTP ${code}"
      echo "query: HTTP ${code}, will retry"
      return 1
      ;;
    *) die "query failed with HTTP ${code}: $(jq -r '.message // empty' "${work}/resp" 2>/dev/null)" ;;
  esac
  local status
  status=$(jq -r '.status // empty' "${work}/resp" 2>/dev/null) || status=""
  if [[ "${status}" != success ]]; then
    die "query returned status '${status}': $(jq -r '.message // empty' "${work}/resp" 2>/dev/null)"
  fi
}

# count_events : sets `found` to how many events match, following continuation
# tokens. Returns 1 if a page failed transiently. Not run in $(...): a die in
# query_page must end the script, not just a subshell.
found=-1 # no query has succeeded yet
count_events() {
  local total=0 token="" n
  while :; do
    query_page "${token}" || return 1
    n=$(jq '.matches | length' "${work}/resp")
    total=$((total + n))
    token=$(jq -r '.continuationToken // empty' "${work}/resp")
    [[ ${n} -eq ${page_size} && -n "${token}" ]] || break
  done
  found=${total}
  last_error=""
}

# ============================================================================
# Phase 4: wait until all events can be queried
# ============================================================================

echo "== Phase 4/4: waiting up to ${QUERY_TIMEOUT}s for ${EXPECTED_EVENTS} events to be queryable"
deadline=$((SECONDS + QUERY_TIMEOUT))
while :; do
  if count_events; then
    echo "query: ${found} of ${EXPECTED_EVENTS} events with param=${PARAM}"
    if [[ ${found} -ge ${EXPECTED_EVENTS} ]]; then
      echo "OK: all ${EXPECTED_EVENTS} events are stored"
      exit 0
    fi
  fi
  if [[ ${SECONDS} -ge ${deadline} ]]; then
    # Say whether the query API itself was failing, so an outage does not
    # look like events that were never stored.
    if [[ ${found} -lt 0 ]]; then
      die "no query succeeded within ${QUERY_TIMEOUT}s; the last one failed with ${last_error}"
    elif [[ -n "${last_error}" ]]; then
      die "only ${found} of ${EXPECTED_EVENTS} events were queryable after ${QUERY_TIMEOUT}s; the last query failed with ${last_error}"
    fi
    die "only ${found} of ${EXPECTED_EVENTS} events were queryable after ${QUERY_TIMEOUT}s"
  fi
  sleep "${QUERY_INTERVAL}"
done
