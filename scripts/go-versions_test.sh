#!/usr/bin/env bash
#
# Tests for scripts/go-versions.sh. Runs offline against fixture repos
# (REPO_ROOT) and fixture release lists (GO_DL_JSON).
#
# Usage: scripts/go-versions_test.sh

set -uo pipefail

script="$(cd "$(dirname "$0")" && pwd)/go-versions.sh"
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

# new_repo NAME GO_MOD_LINE... : creates a fixture repo and prints its path.
new_repo() {
  local dir="${work}/$1"
  shift
  mkdir -p "${dir}"
  {
    echo "module example.com/fixture"
    echo
    for line in "$@"; do echo "${line}"; done
  } >"${dir}/go.mod"
  echo "${dir}"
}

# releases FILE VERSION... : writes a go.dev/dl style release list (all stable).
releases() {
  local file="$1"
  shift
  printf '%s\n' "$@" | jq -R '{version: ., stable: true, files: []}' | jq -s . >"${file}"
}

# run REPO DL_JSON [ARGS...] : stdout in ${out}, stderr in ${err}, status in ${rc}.
run() {
  local repo="$1" dl="$2"
  shift 2
  out=$(REPO_ROOT="${repo}" GO_DL_JSON="${dl}" "${script}" "$@" 2>"${work}/stderr")
  rc=$?
  err=$(cat "${work}/stderr")
}

# expect NAME MATRIX_JSON PRIMARY : last run (default format) succeeded with these values.
expect() {
  local want="primary=$3"$'\n'"matrix=$2"
  if [[ "${rc}" == 0 && "${out}" == "${want}" ]]; then
    pass "$1"
  else
    fail "$1" "rc=${rc}; got '${out}', want '${want}'; stderr: ${err}"
  fi
}

# expect_fail NAME PATTERN : last run failed, printed nothing on stdout, and stderr matches PATTERN.
expect_fail() {
  if [[ "${rc}" != 0 && -z "${out}" ]] && grep -qE "$2" <<<"${err}"; then
    pass "$1"
  else
    fail "$1" "rc=${rc}, stdout='${out}', wanted failure matching /$2/; stderr: ${err}"
  fi
}

dl_current="${work}/dl-current.json"
releases "${dl_current}" go1.27.4 go1.26.11

# --- Happy path --------------------------------------------------------------

repo=$(new_repo happy "go 1.26.0")
run "${repo}" "${dl_current}"
expect "floor 1.26 with supported 1.26/1.27 gives [1.26,1.27]" '["1.26","1.27"]' "1.27"

run "${repo}" "${dl_current}" --json
if [[ "${rc}" == 0 && "$(jq -c . <<<"${out}")" == '{"matrix":["1.26","1.27"],"primary":"1.27"}' ]]; then
  pass "--json prints the same values as JSON"
else
  fail "--json prints the same values as JSON" "rc=${rc} out=${out}"
fi

# --- Sad path ----------------------------------------------------------------

repo=$(new_repo no-directive "require example.com/x v1.0.0")
run "${repo}" "${dl_current}"
expect_fail "go.mod without a go directive is rejected" "could not parse go version"

repo=$(new_repo no-gomod "go 1.26.0")
rm "${repo}/go.mod"
run "${repo}" "${dl_current}"
expect_fail "missing go.mod is rejected" "go.mod not found"

repo=$(new_repo sad "go 1.26.0")
echo "this is not json" >"${work}/dl-bad.json"
run "${repo}" "${work}/dl-bad.json"
expect_fail "malformed release list is rejected" "cannot parse|no stable releases"

echo "[]" >"${work}/dl-empty.json"
run "${repo}" "${work}/dl-empty.json"
expect_fail "empty release list is rejected" "no stable releases"

echo '[{"version":"go1.28rc1","stable":false}]' >"${work}/dl-unstable.json"
run "${repo}" "${work}/dl-unstable.json"
expect_fail "release list with only unstable releases is rejected" "no stable releases"

run "${repo}" "${work}/does-not-exist.json"
expect_fail "unreadable release list file is rejected" "cannot read"

out=$(REPO_ROOT="${repo}" GO_DL_URL="http://127.0.0.1:9/nothing" "${script}" 2>"${work}/stderr")
rc=$?
err=$(cat "${work}/stderr")
expect_fail "unreachable release URL is rejected" "cannot fetch"

run "${repo}" "${dl_current}" --bogus
expect_fail "unknown argument is rejected" "unknown argument"

# --- Edge cases --------------------------------------------------------------

repo=$(new_repo floor-above "go 1.28.0")
run "${repo}" "${dl_current}"
expect "floor newer than every supported release gives [floor]" '["1.28"]' "1.28"

repo=$(new_repo floor-latest "go 1.27.0")
run "${repo}" "${dl_current}"
expect "floor equal to the latest release gives a single entry" '["1.27"]' "1.27"

repo=$(new_repo floor-old "go 1.24.0")
run "${repo}" "${dl_current}"
expect "floor older than both supported releases keeps the floor" '["1.24","1.26","1.27"]' "1.27"

repo=$(new_repo no-patch "go 1.26")
run "${repo}" "${dl_current}"
expect "go directive without a patch number" '["1.26","1.27"]' "1.27"

repo=$(new_repo toolchain "go 1.26.0" "" "toolchain go1.27.1")
run "${repo}" "${dl_current}"
expect "toolchain line does not change the floor" '["1.26","1.27"]' "1.27"

releases "${work}/dl-patches.json" go1.27.4 go1.27.3 go1.26.11 go1.26.10
repo=$(new_repo patches "go 1.26.0")
run "${repo}" "${work}/dl-patches.json"
expect "several patch releases of one minor are deduplicated" '["1.26","1.27"]' "1.27"

releases "${work}/dl-double-digit.json" go1.100.1 go1.99.2
repo=$(new_repo double-digit "go 1.99.0")
run "${repo}" "${work}/dl-double-digit.json"
expect "minors are compared numerically, not lexically" '["1.99","1.100"]' "1.100"

echo '[{"version":"go1.28rc1","stable":false},{"version":"go1.27.4","stable":true}]' >"${work}/dl-mixed.json"
repo=$(new_repo mixed "go 1.27.0")
run "${repo}" "${work}/dl-mixed.json"
expect "unstable releases are ignored" '["1.27"]' "1.27"

# --- Seam: what the workflows consume ----------------------------------------
# The `versions` jobs append stdout to $GITHUB_OUTPUT and feed `matrix` to fromJSON().

repo=$(new_repo seam "go 1.26.0")
run "${repo}" "${dl_current}"
gh_output="${work}/github_output"
printf '%s\n' "${out}" >"${gh_output}"
lines=$(wc -l <"${gh_output}" | tr -d ' ')
keys=$(cut -d= -f1 "${gh_output}" | paste -sd, -)
matrix=$(sed -n 's/^matrix=//p' "${gh_output}")
if [[ "${lines}" == 2 && "${keys}" == "primary,matrix" ]] &&
  jq -e 'type == "array" and all(type == "string")' <<<"${matrix}" >/dev/null; then
  pass "output is two key=value lines and matrix is a JSON string array"
else
  fail "output is two key=value lines and matrix is a JSON string array" "$(cat "${gh_output}")"
fi

# --- Conformance -------------------------------------------------------------

if jq -e --arg floor "1.26" '
     def num: split(".") | map(tonumber);
     all(test("^[0-9]+\\.[0-9]+$"))
       and (. == (unique_by(num) | sort_by(num)))
       and all(num >= ($floor | num))' <<<"${matrix}" >/dev/null; then
  pass "matrix entries are <major>.<minor>, sorted, unique and >= floor"
else
  fail "matrix entries are <major>.<minor>, sorted, unique and >= floor" "${matrix}"
fi

if [[ "$(sed -n 's/^primary=//p' "${gh_output}")" == "$(jq -r '.[-1]' <<<"${matrix}")" ]]; then
  pass "primary is the highest matrix entry"
else
  fail "primary is the highest matrix entry" "$(cat "${gh_output}")"
fi

echo
echo "${passed} passed, ${failed} failed"
[[ "${failed}" == 0 ]]
