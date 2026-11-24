#!/usr/bin/env bash
#
# Tests for scripts/run-example-client.sh. Runs offline: the script is copied
# into a fixture tree, and fakes first on PATH stand in for the network and the
# build. `go build` writes a stub client that records how it was called, and
# `curl` answers from a queue of canned responses and records every request.
# jq is the real one.
#
# Usage: scripts/run-example-client_test.sh

set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
script="${here}/run-example-client.sh"
work=$(mktemp -d)
work="$(cd "${work}" && pwd -P)"
trap 'rm -rf "${work}"' EXIT

passed=0
failed=0

pass() {
  passed=$((passed + 1))
  echo "ok   - $1"
}
fail() {
  failed=$((failed + 1))
  echo "FAIL - $1" >&2
  if [[ -n "${2:-}" ]]; then echo "       $2" >&2; fi
}
# COND; check NAME DETAIL : passes if COND (the command just before) succeeded.
# DETAIL must not contain $(...): a command substitution in the arguments
# replaces $? before check can read it.
check() {
  local status=$?
  if [[ ${status} -eq 0 ]]; then pass "$1"; else fail "$1" "$2"; fi
}

command -v jq >/dev/null || {
  echo "FAIL - jq is required to run these tests" >&2
  exit 1
}

mkdir -p "${work}/bin"
# Fake `go build -o OUT .`: writes a stub client into OUT. The stub logs its
# arguments and the env it got to CLIENT_LOG, and exits with STUB_CLIENT_EXIT.
cat >"${work}/bin/go" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == build ]] || { echo "fake go: unexpected: $*" >&2; exit 2; }
[[ -z "${STUB_BUILD_FAIL:-}" ]] || { echo "fake go: build failed" >&2; exit 1; }
echo "${PWD}" >"${CLIENT_LOG}.builddir"
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == -o ]]; then out="$2"; shift; fi
  shift
done
cat >"${out}" <<'STUB'
#!/usr/bin/env bash
{
  echo "args=$*"
  echo "server=${SCALYR_SERVER-unset}"
  echo "write=${SCALYR_WRITELOG_TOKEN-unset}"
} >"${CLIENT_LOG}"
exit "${STUB_CLIENT_EXIT:-0}"
STUB
chmod +x "${out}"
EOF

# Fake curl. Response N comes from ${RESP_DIR}/N.code and N.body (the last one
# repeats). A code of "fail" makes curl exit 7, like a refused connection.
# Each call appends one line of arguments to CURL_LOG, the posted body to
# CURL_LOG.bodies, and the -H @file content and permissions to CURL_LOG.auth.
cat >"${work}/bin/curl" <<'EOF'
#!/usr/bin/env bash
n=$(( $(cat "${RESP_DIR}/counter" 2>/dev/null || echo 0) + 1 ))
echo "${n}" >"${RESP_DIR}/counter"
pick="${n}"
while [[ ! -f "${RESP_DIR}/${pick}.code" && ${pick} -gt 1 ]]; do pick=$((pick - 1)); done
printf '%s\n' "$*" >>"${CURL_LOG}"
out="" fmt="" data=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) out="$2"; shift ;;
    -w) fmt="$2"; shift ;;
    --data-binary) data="$2"; shift ;;
    -H)
      if [[ "$2" == @* ]]; then
        f="${2#@}"
        perms=$(stat -c %a "${f}" 2>/dev/null || stat -f %Lp "${f}")
        echo "perms=${perms} $(cat "${f}")" >>"${CURL_LOG}.auth"
      fi
      shift ;;
  esac
  shift
done
echo "${data}" >>"${CURL_LOG}.bodies"
code=$(cat "${RESP_DIR}/${pick}.code")
[[ "${code}" != fail ]] || { echo "curl: (7) Failed to connect" >&2; exit 7; }
cp "${RESP_DIR}/${pick}.body" "${out}"
[[ "${fmt}" != '%{http_code}' ]] || printf '%s' "${code}"
EOF
chmod +x "${work}/bin/go" "${work}/bin/curl"

# new_tree NAME : a fixture repo layout holding the script; prints its path.
new_tree() {
  local dir="${work}/$1"
  mkdir -p "${dir}/scripts" "${dir}/examples/client"
  cp "${script}" "${dir}/scripts/"
  echo "${dir}"
}
tree=$(new_tree "repo tree")

# matches N [TOKEN] : a successful query response body with N matches.
matches() {
  jq -cn --argjson n "$1" --arg t "${2:-}" \
    '{status: "success", matches: [range($n) | {message: "m"}]} + (if $t == "" then {} else {continuationToken: $t} end)'
}

# responses CODE BODY [CODE BODY...] : queue up curl's answers, in order.
responses() {
  rm -rf "${work}/resp"
  mkdir -p "${work}/resp"
  local i=1
  while [[ $# -gt 0 ]]; do
    echo "$1" >"${work}/resp/${i}.code"
    printf '%s' "$2" >"${work}/resp/${i}.body"
    i=$((i + 1))
    shift 2
  done
}

# run [ARGS...] : runs the script with valid defaults (override with env);
# sets out, rc, client (the stub's log), curls (one line per request),
# bodies, auth.
run() {
  rm -f "${work}/client.log" "${work}/client.log.builddir" "${work}/curl.log" "${work}/curl.log.bodies" "${work}/curl.log.auth"
  out=$(PATH="${work}/bin:${PATH}" CLIENT_LOG="${work}/client.log" CURL_LOG="${work}/curl.log" RESP_DIR="${work}/resp" \
    SCALYR_SERVER="${SCALYR_SERVER-https://dataset.invalid}" \
    SCALYR_WRITELOG_TOKEN="${SCALYR_WRITELOG_TOKEN-write-secret}" \
    SCALYR_READLOG_TOKEN="${SCALYR_READLOG_TOKEN-read-secret}" \
    EXPECTED_EVENTS="${EXPECTED_EVENTS-3}" QUERY_TIMEOUT="${QUERY_TIMEOUT-5}" QUERY_INTERVAL="${QUERY_INTERVAL-1}" \
    "${tree}/scripts/run-example-client.sh" "$@" 2>&1)
  rc=$?
  client=$(cat "${work}/client.log" 2>/dev/null)
  curls=$(cat "${work}/curl.log" 2>/dev/null)
  bodies=$(cat "${work}/curl.log.bodies" 2>/dev/null)
  auth=$(cat "${work}/curl.log.auth" 2>/dev/null)
}
ncurls() {
  if [[ -z "${curls}" ]]; then echo 0; else wc -l <<<"${curls}" | tr -d ' '; fi
}

# --- happy: the client runs with the param, then all events are found ---
responses 200 "$(matches 3)"
PARAM=run-1 run
[[ ${rc} -eq 0 && "${out}" == *"OK: all 3 events are stored"* ]]
check "all events found on the first poll succeeds" "rc=${rc} out=${out}"
[[ "${client}" == $'args=-param run-1\nserver=https://dataset.invalid\nwrite=write-secret' ]]
check "the client gets -param and the server and write token from the environment" "client=${client}"
builddir=$(cat "${work}/client.log.builddir")
[[ "${builddir}" == "${tree}/examples/client" ]]
check "the client is built in examples/client" "builddir=${builddir}"

# --- conformance: the query API request (https://app.scalyr.com/help/api#query) ---
req=$(head -1 <<<"${bodies}")
jq -e '.queryType == "log" and .filter == "param == '"'"'run-1'"'"'" and .maxCount == 5000 and .startTime == "1h" and (has("continuationToken") | not)' <<<"${req}" >/dev/null
check "the query is a log query filtered on the param, at most 5000 per page" "body=${req}"
[[ "${curls}" == *"-X POST https://dataset.invalid/api/query"* && "${curls}" == *"Content-Type: application/json"* ]]
check "it is POSTed as JSON to SCALYR_SERVER/api/query" "curls=${curls}"

# --- seam/security: the read token goes in a private header file, never in argv or output ---
[[ "${auth}" == "perms=600 Authorization: Bearer read-secret" ]]
check "the read token is sent as a Bearer header from a mode 600 file" "auth=${auth}"
[[ "${curls}" != *secret* && "${out}" != *secret* ]]
check "no token appears on curl's command line or in the output" "curls=${curls}"

# --- happy: events that appear later are waited for ---
responses 200 "$(matches 0)" 200 "$(matches 1)" 200 "$(matches 3)"
run
[[ ${rc} -eq 0 && "$(ncurls)" == 3 && "${out}" == *"0 of 3"*"1 of 3"*"OK"* ]]
check "polls until all events are queryable" "rc=${rc} out=${out}"

# --- happy: more than one page is followed through continuation tokens ---
responses 200 "$(matches 5000 tok-1)" 200 "$(matches 5000 tok-2)" 200 "$(matches 2 tok-3)"
EXPECTED_EVENTS=10002 run
[[ ${rc} -eq 0 && "${out}" == *"10002 of 10002"* && "$(ncurls)" == 3 ]]
check "pages are followed with continuationToken and summed" "rc=${rc} out=${out}"
[[ "$(jq -r '.continuationToken // "-"' <<<"${bodies}" | tr '\n' ' ')" == "- tok-1 tok-2 " ]]
check "each next page sends the previous page's continuationToken" "bodies=${bodies}"

# --- edge: a full last page without a token ends the paging ---
responses 200 "$(matches 5000)"
EXPECTED_EVENTS=5000 run
[[ ${rc} -eq 0 && "$(ncurls)" == 1 ]]
check "a full page without a continuationToken is the last page" "rc=${rc} curls=${curls}"

# --- edge: more events than expected (e.g. a reused PARAM) still passes ---
responses 200 "$(matches 4)"
run
[[ ${rc} -eq 0 && "${out}" == *"4 of 3"* ]]
check "finding more events than expected passes" "rc=${rc} out=${out}"

# --- edge: transient failures are retried ---
responses 503 '{"status":"error/server"}' fail "" 200 "$(matches 3)"
run
[[ ${rc} -eq 0 && "$(ncurls)" == 3 && "${out}" == *"HTTP 503, will retry"* && "${out}" == *"HTTP 000, will retry"* ]]
check "HTTP 5xx and connection failures are retried" "rc=${rc} out=${out}"

# --- edge: a trailing slash on SCALYR_SERVER is not doubled ---
responses 200 "$(matches 3)"
SCALYR_SERVER=https://dataset.invalid/ run
[[ ${rc} -eq 0 && "${curls}" == *" https://dataset.invalid/api/query "* ]]
check "a trailing slash on SCALYR_SERVER is dropped" "curls=${curls}"

# --- edge: the default PARAM is unique per run and safe for the filter ---
responses 200 "$(matches 3)"
run
p1=$(sed -n 's/^args=-param //p' <<<"${client}")
run
p2=$(sed -n 's/^args=-param //p' <<<"${client}")
[[ "${p1}" =~ ^dataset-go-e2e-[0-9]+-[0-9]+$ && "${p1}" != "${p2}" ]]
check "the default PARAM is filter-safe and differs between runs" "p1=${p1} p2=${p2}"

# --- sad: a rejected query fails at once with the server's message ---
responses 401 '{"message":"authorization token [...token] is invalid","status":"error/client/badParam"}'
QUERY_TIMEOUT=60 run
[[ ${rc} -eq 1 && "$(ncurls)" == 1 && "${out}" == *"HTTP 401: authorization token [...token] is invalid"* ]]
check "HTTP 401 fails immediately with the server's message, without retrying" "rc=${rc} out=${out}"

responses 200 '{"status":"error/client/badParam","message":"bad filter"}'
QUERY_TIMEOUT=60 run
[[ ${rc} -eq 1 && "$(ncurls)" == 1 && "${out}" == *"status 'error/client/badParam': bad filter"* ]]
check "a non-success status in a 200 response fails immediately" "rc=${rc} out=${out}"

responses 200 'not json'
QUERY_TIMEOUT=60 run
[[ ${rc} -eq 1 && "$(ncurls)" == 1 && "${out}" == *"status ''"* ]]
check "a response that is not JSON fails immediately" "rc=${rc} out=${out}"

# --- sad: missing events fail after QUERY_TIMEOUT ---
responses 200 "$(matches 2)"
QUERY_TIMEOUT=0 run
[[ ${rc} -eq 1 && "${out}" == *"only 2 of 3 events were queryable after 0s"* ]]
check "too few events fail after QUERY_TIMEOUT with the count" "rc=${rc} out=${out}"

# --- sad: an unreachable query API is reported as such, not as missing events ---
responses 503 '{"status":"error/server"}'
QUERY_TIMEOUT=0 run
[[ ${rc} -eq 1 && "${out}" == *"no query succeeded within 0s; the last one failed with HTTP 503"* ]]
check "a query API that never answers is reported as failing queries" "rc=${rc} out=${out}"

responses 200 "$(matches 2)" 503 '{"status":"error/server"}'
QUERY_TIMEOUT=2 run
[[ ${rc} -eq 1 && "${out}" == *"only 2 of 3 events were queryable after 2s; the last query failed with HTTP 503"* ]]
check "a timeout after a failed last query gives the last count and the failure" "rc=${rc} out=${out}"

responses 503 '{"status":"error/server"}' 200 "$(matches 2)"
QUERY_TIMEOUT=2 run
[[ ${rc} -eq 1 && "${out}" == *"only 2 of 3 events were queryable after 2s" && "${out}" != *"last query failed"* ]]
check "an earlier failure is not reported once a later query succeeded" "rc=${rc} out=${out}"

# --- conformance: the phases run and are logged in order ---
responses 200 "$(matches 3)"
run
[[ "${out}" == *"== Phase 1/4: building"*"== Phase 2/4: sending events"*"== Phase 3/4: preparing queries"*"== Phase 4/4: waiting"*"OK: all 3"* ]]
check "the phases are logged in order: build, send, wait and query" "out=${out}"

# --- sad: the client or its build failing stops before any query ---
responses 200 "$(matches 3)"
STUB_CLIENT_EXIT=3 run
[[ ${rc} -eq 1 && "${out}" == *"examples/client failed"* && "$(ncurls)" == 0 ]]
check "a failing client fails the script before querying" "rc=${rc} out=${out}"
STUB_BUILD_FAIL=1 run
[[ ${rc} -ne 0 && -z "${client}" && "$(ncurls)" == 0 ]]
check "a failing build fails the script before running the client" "rc=${rc} out=${out}"

# --- sad: required environment and invalid settings are rejected up front ---
for var in SCALYR_SERVER SCALYR_WRITELOG_TOKEN SCALYR_READLOG_TOKEN; do
  eval "${var}= run"
  [[ ${rc} -eq 1 && "${out}" == *"${var} is not set"* && -z "${client}" && "$(ncurls)" == 0 ]]
check "an empty ${var} is rejected before anything runs" "rc=${rc} out=${out}"
done
for bad in "PARAM=it's" "PARAM=a b" "EXPECTED_EVENTS=0" "EXPECTED_EVENTS=x" "QUERY_INTERVAL=0" "QUERY_TIMEOUT=-1"; do
  eval "$(printf '%q' "${bad%%=*}")=$(printf '%q' "${bad#*=}") run"
  [[ ${rc} -eq 1 && -z "${client}" ]]
check "rejects ${bad}" "rc=${rc} out=${out}"
done
run extra
[[ ${rc} -eq 2 && "${out}" == *Usage* && -z "${client}" ]]
check "rejects arguments" "rc=${rc} out=${out}"

# --- conformance: the defaults match the real example client ---
main="${here}/../examples/client/main.go"
bundles=$(sed -n 's/^[[:space:]]*BundleCount[[:space:]]*=[[:space:]]*\([0-9]*\).*/\1/p' "${main}")
grep -q "EXPECTED_EVENTS:-${bundles}}" "${script}"
check "the default EXPECTED_EVENTS equals examples/client BundleCount" "BundleCount=${bundles}"
grep -q 'flag.String("param"' "${main}" && grep -q 'event.Attrs\["param"\] = \*param' "${main}"
check "examples/client defines -param and stores it as attribute \"param\"" ""

echo
echo "${passed} passed, ${failed} failed"
[[ ${failed} -eq 0 ]]
