#!/usr/bin/env bash
#
# Tests for scripts/bump-version.sh. Rewrites temp copies of
# pkg/version/version.go (VERSION_FILE); the seam case builds and tests a copy
# of the module with the rewritten file.
#
# Usage: scripts/bump-version_test.sh

set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
script="${here}/bump-version.sh"
real_root="$(cd "${here}/.." && pwd)"
real_file="${real_root}/pkg/version/version.go"
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

# fixture NAME [CONTENT] : a version.go copy (default: the real file). Prints its path.
fixture() {
  local file="${work}/$1.go"
  if [[ $# -ge 2 ]]; then printf '%s\n' "$2" >"${file}"; else cp "${real_file}" "${file}"; fi
  echo "${file}"
}

# run FILE ARGS... : stdout+stderr in ${err}, status in ${rc}.
run() {
  local file="$1"
  shift
  err=$(VERSION_FILE="${file}" "${script}" "$@" 2>&1)
  rc=$?
}

# expect_values NAME FILE VERSION DATE : succeeded and the file holds these values.
expect_values() {
  local v d
  v=$(sed -nE 's/^[[:space:]]*Version[[:space:]]*=[[:space:]]*"([^"]*)".*/\1/p' "$2")
  d=$(sed -nE 's/^[[:space:]]*ReleasedDate[[:space:]]*=[[:space:]]*"([^"]*)".*/\1/p' "$2")
  if [[ "${rc}" == 0 && "${v}" == "$3" && "${d}" == "$4" ]]; then
    pass "$1"
  else
    fail "$1" "rc=${rc}, Version='${v}' ReleasedDate='${d}', want '$3' '$4'; output: ${err}"
  fi
}

# expect_fail NAME FILE ORIGINAL PATTERN : failed, output matches PATTERN, and
# the file is byte-identical to ORIGINAL (or still absent).
expect_fail() {
  local unchanged=true
  if [[ -e "$3" ]]; then cmp -s "$2" "$3" || unchanged=false; else [[ ! -e "$2" ]] || unchanged=false; fi
  if [[ "${rc}" != 0 && "${unchanged}" == true ]] && grep -qE "$4" <<<"${err}"; then
    pass "$1"
  else
    fail "$1" "rc=${rc}, unchanged=${unchanged}, wanted failure matching /$4/; output: ${err}"
  fi
}

# --- Happy path --------------------------------------------------------------

file=$(fixture happy)
run "${file}" 0.21.1 2026-11-11
expect_values "rewrites Version and ReleasedDate" "${file}" 0.21.1 2026-11-11
sed -E -e 's/^([[:space:]]*Version[[:space:]]*=[[:space:]]*)"[^"]*"/\1"0.21.1"/' \
  -e 's/^([[:space:]]*ReleasedDate[[:space:]]*=[[:space:]]*)"[^"]*"/\1"2026-11-11"/' \
  "${real_file}" >"${work}/want.go"
if diff -u "${work}/want.go" "${file}" >"${work}/diff"; then
  pass "changes nothing else in the file"
else
  fail "changes nothing else in the file" "$(cat "${work}/diff")"
fi

# --- Sad path ----------------------------------------------------------------

for bad in v0.21.1 0.21 0.21.1-rc1 0.21.1.2 01.2.3 "" " 0.21.1" abc; do
  file=$(fixture "bad-version")
  cp "${file}" "${work}/orig.go"
  run "${file}" "${bad}" 2026-11-11
  expect_fail "version '${bad}' is rejected" "${file}" "${work}/orig.go" "is not X.Y.Z"
done

for bad in 2026-1-11 11.11.2026 2026/11/11 "" today; do
  file=$(fixture "bad-date")
  cp "${file}" "${work}/orig.go"
  run "${file}" 0.21.1 "${bad}"
  expect_fail "date '${bad}' is rejected" "${file}" "${work}/orig.go" "is not YYYY-MM-DD"
done

file=$(fixture args)
cp "${file}" "${work}/orig.go"
run "${file}" 0.21.1
expect_fail "missing date argument is rejected" "${file}" "${work}/orig.go" "usage"
run "${file}" 0.21.1 2026-11-11 extra
expect_fail "extra argument is rejected" "${file}" "${work}/orig.go" "usage"

run "${work}/does-not-exist.go" 0.21.1 2026-11-11
expect_fail "missing file is rejected" "${work}/does-not-exist.go" "${work}/does-not-exist.go" "not found"

file=$(fixture no-version $'package version\n\nconst (\n\tReleasedDate = "2024-06-27"\n)')
cp "${file}" "${work}/orig.go"
run "${file}" 0.21.1 2026-11-11
expect_fail "file without a Version line is rejected" "${file}" "${work}/orig.go" "exactly one 'Version"

file=$(fixture two-versions $'package version\n\nconst (\n\tVersion      = "0.1.0"\n\tReleasedDate = "2024-06-27"\n)\n\nconst (\n\tVersion = "0.2.0"\n)')
cp "${file}" "${work}/orig.go"
run "${file}" 0.21.1 2026-11-11
expect_fail "file with two Version lines is rejected" "${file}" "${work}/orig.go" "found 2"

file=$(fixture no-date $'package version\n\nconst (\n\tVersion = "0.1.0"\n)')
cp "${file}" "${work}/orig.go"
run "${file}" 0.21.1 2026-11-11
expect_fail "file without a ReleasedDate line is rejected" "${file}" "${work}/orig.go" "exactly one 'ReleasedDate"

# Version as an expression, not a string literal: not something to rewrite blindly.
file=$(fixture computed $'package version\n\nconst (\n\tVersion      = prefix + "0.1.0"\n\tReleasedDate = "2024-06-27"\n)\n\nconst prefix = ""')
cp "${file}" "${work}/orig.go"
run "${file}" 0.21.1 2026-11-11
expect_fail "non-literal Version is rejected" "${file}" "${work}/orig.go" "exactly one 'Version"

# --- Edge cases --------------------------------------------------------------

file=$(fixture multi-digit)
run "${file}" 10.200.3000 2026-11-11
expect_values "multi-digit components are accepted" "${file}" 10.200.3000 2026-11-11

file=$(fixture zeros)
run "${file}" 0.0.0 2026-01-01
expect_values "0.0.0 is accepted" "${file}" 0.0.0 2026-01-01

file=$(fixture rerun)
run "${file}" 0.21.1 2026-11-11
cp "${file}" "${work}/first.go"
run "${file}" 0.21.1 2026-11-11
if [[ "${rc}" == 0 ]] && cmp -s "${file}" "${work}/first.go"; then
  pass "re-running with the same values is a no-op"
else
  fail "re-running with the same values is a no-op" "rc=${rc}; output: ${err}"
fi

file=$(fixture mode)
chmod 0640 "${file}"
run "${file}" 0.21.1 2026-11-11
# find -perm is the portable check (stat flags differ between BSD and GNU).
if [[ "${rc}" == 0 && -n "$(find "${file}" -perm 0640)" ]]; then
  pass "file mode is preserved"
else
  fail "file mode is preserved" "rc=${rc}; $(ls -l "${file}")"
fi

# --- Conformance -------------------------------------------------------------

file=$(fixture conformance)
run "${file}" 0.21.1 2026-11-11
if [[ "${rc}" == 0 && -z "$(gofmt -l "${file}")" ]]; then
  pass "rewritten file is gofmt-clean"
else
  fail "rewritten file is gofmt-clean" "rc=${rc}; $(gofmt -d "${file}")"
fi
if [[ "$(head -n 15 "${file}")" == "$(head -n 15 "${real_file}")" ]]; then
  pass "license header is unchanged"
else
  fail "license header is unchanged"
fi

# --- Seam: the rewritten constant reaches the client ---------------------------

mod="${work}/module"
mkdir -p "${mod}"
cp -R "${real_root}/go.mod" "${real_root}/go.sum" "${real_root}/pkg" "${real_root}/internal" "${mod}/"
run "${mod}/pkg/version/version.go" 0.21.1 2026-11-11
if [[ "${rc}" == 0 ]] && (cd "${mod}" && go build ./pkg/version ./pkg/client &&
  go test -count=1 ./pkg/version && go test -count=1 -run 'UserAgent' ./pkg/client) >"${work}/seam" 2>&1; then
  pass "module builds and version + User-Agent tests pass with the rewritten version"
else
  fail "module builds and version + User-Agent tests pass with the rewritten version" "rc=${rc}; $(cat "${work}/seam")"
fi

echo
echo "${passed} passed, ${failed} failed"
[[ "${failed}" == 0 ]]
