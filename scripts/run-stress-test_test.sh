#!/usr/bin/env bash
#
# Tests for scripts/run-stress-test.sh. Runs offline: the script is copied into
# a fixture tree whose `go` (first on PATH) "builds" a stub stress binary that
# records its arguments and exits with STUB_EXIT.
#
# Usage: scripts/run-stress-test_test.sh

set -uo pipefail

script="$(cd "$(dirname "$0")" && pwd)/run-stress-test.sh"
work=$(mktemp -d)
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

# Fake `go`: `go build -race -o stress` writes a stub that records its args.
# With STUB_BUILD_FAIL set, the build fails and writes nothing.
mkdir -p "${work}/bin"
cat >"${work}/bin/go" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == build ]] || { echo "fake go: unexpected: $*" >&2; exit 2; }
[[ -z "${STUB_BUILD_FAIL:-}" ]] || { echo "fake go: build failed" >&2; exit 1; }
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == -o ]]; then out="$2"; shift; fi
  shift
done
cat >"${out}" <<'STUB'
#!/usr/bin/env bash
echo "$*" >args.txt
echo "stub stress output"
exit "${STUB_EXIT:-0}"
STUB
chmod +x "${out}"
EOF
chmod +x "${work}/bin/go"

# new_tree NAME : a fixture repo layout with the script under test; prints its path.
new_tree() {
  local dir="${work}/$1"
  mkdir -p "${dir}/scripts" "${dir}/examples/stress"
  cp "${script}" "${dir}/scripts/"
  echo "${dir}"
}

# run TREE ARGS... : runs the script in TREE, captures output and exit code.
run() {
  local tree="$1"
  shift
  out=$(PATH="${work}/bin:${PATH}" "${tree}/scripts/run-stress-test.sh" "$@" 2>&1)
  rc=$?
}

# args_are TREE EXPECTED : the stub in TREE was called with exactly EXPECTED.
args_are() {
  [[ "$(cat "$1/examples/stress/args.txt" 2>/dev/null)" == "$2" ]]
}

# --- happy path: explicit arguments reach the binary ---
t=$(new_tree explicit)
run "${t}" 123 45
if [[ ${rc} -eq 0 ]] && args_are "${t}" "--events=123 --buckets=45 --sleep=10ms"; then
  pass "explicit arguments are passed as --events/--buckets"
else
  fail "explicit arguments are passed as --events/--buckets" "rc=${rc} out=${out}"
fi

# --- edge: no arguments use the defaults ---
t=$(new_tree defaults)
run "${t}"
if [[ ${rc} -eq 0 ]] && args_are "${t}" "--events=10000 --buckets=10000 --sleep=10ms"; then
  pass "no arguments default to 10000 events and 10000 buckets"
else
  fail "no arguments default to 10000 events and 10000 buckets" "rc=${rc} out=${out}"
fi

# --- edge: -1 buckets (max congestion, used by CI) is accepted ---
t=$(new_tree congestion)
run "${t}" 10000 -1
if [[ ${rc} -eq 0 ]] && args_are "${t}" "--events=10000 --buckets=-1 --sleep=10ms"; then
  pass "-1 buckets is accepted"
else
  fail "-1 buckets is accepted" "rc=${rc} out=${out}"
fi

# --- sad: a failing stress run fails the script (not masked by tee) ---
t=$(new_tree failing)
STUB_EXIT=3 run "${t}" 10 10
if [[ ${rc} -eq 3 && "${out}" == *"stub stress output"* ]]; then
  pass "the stress binary's exit status is propagated"
else
  fail "the stress binary's exit status is propagated" "rc=${rc} out=${out}"
fi

# --- sad: a failing build fails the script, and no stress run happens ---
t=$(new_tree build-fails)
STUB_BUILD_FAIL=1 run "${t}" 10 10
if [[ ${rc} -ne 0 && "${out}" == *"build failed"* && ! -e "${t}/examples/stress/args.txt" ]]; then
  pass "a failing build makes the script fail without running stress"
else
  fail "a failing build makes the script fail without running stress" "rc=${rc} out=${out}"
fi

# --- sad: non-integer arguments are rejected before building ---
for args in "abc 10" "10 x" "10 -2" "-1 10" "0 10"; do
  t=$(new_tree "bad-${args// /_}")
  # shellcheck disable=SC2086 # split on purpose
  run "${t}" ${args}
  if [[ ${rc} -ne 0 && "${out}" == *Usage* && ! -e "${t}/examples/stress/stress" ]]; then
    pass "rejects arguments '${args}'"
  else
    fail "rejects arguments '${args}'" "rc=${rc} out=${out}"
  fi
done

# --- sad: an explicitly empty argument or a third argument is rejected ---
t=$(new_tree empty-arg)
run "${t}" "" 10
if [[ ${rc} -ne 0 && "${out}" == *Usage* ]]; then
  pass "rejects an empty argument"
else
  fail "rejects an empty argument" "rc=${rc} out=${out}"
fi
t=$(new_tree extra-arg)
run "${t}" 10 10 10
if [[ ${rc} -ne 0 && "${out}" == *Usage* ]]; then
  pass "rejects a third argument"
else
  fail "rejects a third argument" "rc=${rc} out=${out}"
fi

# --- seam: the log lands where CI's upload-artifact glob (examples/stress/*.log) looks ---
t=$(new_tree log)
run "${t}" 7 8
if grep -q "stub stress output" "${t}/examples/stress/out-7-8.log" 2>/dev/null; then
  pass "output is logged to examples/stress/out-7-8.log"
else
  fail "output is logged to examples/stress/out-7-8.log" "rc=${rc} out=${out}"
fi

# --- conformance: every invocation in CI and the Makefile passes validation ---
# Runs each real invocation against the fixture, so a caller and the script's
# argument rules cannot drift apart unnoticed.
repo="$(cd "$(dirname "$0")/.." && pwd)"
invocations=$(
  {
    grep -hoE 'run-stress-test\.sh( +[^ ]+){0,2}$' "${repo}/.github/workflows/stress-tests.yaml"
    make -s -n -C "${repo}" stress-test | grep -oE 'run-stress-test\.sh( +[^ ]+){0,2}$'
  } | sed 's/^run-stress-test\.sh *//'
)
count=$(grep -c . <<<"${invocations}")
if [[ ${count} -ge 4 ]]; then
  pass "found ${count} invocations in stress-tests.yaml and the Makefile"
else
  fail "found ${count} invocations in stress-tests.yaml and the Makefile, expected at least 4"
fi
while read -r args; do
  t=$(new_tree "caller-${args// /_}")
  # shellcheck disable=SC2086 # split on purpose
  run "${t}" ${args}
  if [[ ${rc} -eq 0 ]]; then
    pass "caller invocation '${args}' is accepted"
  else
    fail "caller invocation '${args}' is accepted" "rc=${rc} out=${out}"
  fi
done <<<"${invocations}"

echo
echo "${passed} passed, ${failed} failed"
[[ ${failed} -eq 0 ]]
