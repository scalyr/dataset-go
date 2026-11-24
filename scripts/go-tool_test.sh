#!/usr/bin/env bash
# Copyright 2026 SentinelOne, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tests for scripts/go-tool.sh. The fake-go cases run offline. The seam case
# runs a real pinned tool from a directory that has a vendor/, which is the
# setup that breaks `go tool -modfile` in CI; it needs the Go module cache
# (or network) and is skipped without go.
#
# Usage: scripts/go-tool_test.sh

set -uo pipefail

scripts_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${scripts_dir}/.." && pwd)"
script="${scripts_dir}/go-tool.sh"
work="$(mktemp -d)/with space"
mkdir -p "${work}"
trap 'rm -rf "$(dirname "${work}")"' EXIT

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

# A fake `go` whose `tool -n` prints the path of a fake tool, which records
# its working directory and arguments, one per line in brackets (so an empty
# argument is visible).
bin="${work}/bin"
mkdir -p "${bin}"
cat >"${bin}/go" <<FAKE
#!/usr/bin/env bash
echo "go \$*" >>"${work}/go-calls"
[[ "\${FAKE_GO_FAIL:-}" == 1 ]] && { echo "go: no such tool" >&2; exit 1; }
echo "${work}/fake-tool"
FAKE
cat >"${work}/fake-tool" <<FAKE
#!/usr/bin/env bash
pwd >"${work}/tool-calls"
printf '[%s]\n' "\$@" >>"${work}/tool-calls"
exit "\${FAKE_TOOL_STATUS:-0}"
FAKE
chmod +x "${bin}/go" "${work}/fake-tool"

# run [ARGS...] : runs the script from ${cwd} (default: a scratch dir), with the fake go.
run() {
  : >"${work}/go-calls"
  : >"${work}/tool-calls"
  out=$(cd "${cwd:-${work}}" && PATH="${bin}:${PATH}" "${script}" "$@" 2>&1)
  rc=$?
}

# --- Happy path --------------------------------------------------------------

run errcheck ./...
if [[ "${rc}" == 0 && "$(cat "${work}/go-calls")" == "go -C ${repo_root}/tools tool -n errcheck" ]]; then
  pass "resolves the tool inside tools/"
else
  fail "resolves the tool inside tools/" "rc=${rc}; go calls: $(cat "${work}/go-calls"); output: ${out}"
fi
if [[ "$(cat "${work}/tool-calls")" == "${work}"$'\n'"[./...]" ]]; then
  pass "runs the tool in the caller's directory with the remaining arguments"
else
  fail "runs the tool in the caller's directory with the remaining arguments" "$(cat "${work}/tool-calls")"
fi

# --- Sad path ----------------------------------------------------------------

FAKE_TOOL_STATUS=3 run errcheck
if [[ "${rc}" == 3 ]]; then
  pass "passes the tool's exit status through"
else
  fail "passes the tool's exit status through" "rc=${rc}"
fi

FAKE_GO_FAIL=1 run nosuchtool
if [[ "${rc}" != 0 && ! -s "${work}/tool-calls" ]] && grep -q "no such tool" <<<"${out}"; then
  pass "fails, without running anything, when go cannot resolve the tool"
else
  fail "fails, without running anything, when go cannot resolve the tool" "rc=${rc}; output: ${out}"
fi

run
if [[ "${rc}" == 2 && -z "$(cat "${work}/go-calls")" ]] && grep -q "usage:" <<<"${out}"; then
  pass "prints usage and exits 2 without a tool name"
else
  fail "prints usage and exits 2 without a tool name" "rc=${rc}; output: ${out}"
fi

# --- Edge cases --------------------------------------------------------------

cat >"${bin}/go" <<FAKE
#!/usr/bin/env bash
echo "GOFLAGS=[\${GOFLAGS-unset}] GOWORK=[\${GOWORK-unset}]" >"${work}/go-env"
echo "${work}/fake-tool"
FAKE
GOFLAGS=-mod=vendor GOWORK="${work}/go.work" run errcheck
if [[ "$(cat "${work}/go-env")" == "GOFLAGS=[] GOWORK=[off]" ]]; then
  pass "ignores the caller's GOFLAGS and go.work when resolving the tool"
else
  fail "ignores the caller's GOFLAGS and go.work when resolving the tool" "$(cat "${work}/go-env")"
fi

run gofumpt -w "a file.go" ""
if [[ "$(sed -n '2,$p' "${work}/tool-calls")" == $'[-w]\n[a file.go]\n[]' ]]; then
  pass "keeps arguments with spaces and empty arguments intact"
else
  fail "keeps arguments with spaces and empty arguments intact" "$(cat "${work}/tool-calls")"
fi

# --- Seam: a real pinned tool, next to a vendor/ directory ---------------------

if command -v go >/dev/null; then
  mod="${work}/vendored"
  mkdir -p "${mod}/vendor"
  printf 'module example.com/vendored\n\ngo 1.26.0\n\nrequire example.com/dep v1.0.0\n' >"${mod}/go.mod"
  printf '# example.com/dep v1.0.0\n' >"${mod}/vendor/modules.txt"
  printf 'package vendored\n' >"${mod}/a.go"
  if seam_out=$(cd "${mod}" && "${script}" gofumpt -l a.go 2>&1); then
    pass "runs a pinned tool in a module with a mismatched vendor/"
  else
    fail "runs a pinned tool in a module with a mismatched vendor/" "${seam_out}"
  fi
else
  if [[ -n "${CI:-}" ]]; then
    fail "runs a pinned tool in a module with a mismatched vendor/" "skipped on CI: go not installed"
  else
    echo "skip - go not installed; real tool not run"
  fi
fi

echo
echo "${passed} passed, ${failed} failed"
[[ "${failed}" == 0 ]]
