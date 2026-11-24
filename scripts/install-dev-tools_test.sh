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
# Tests for scripts/install-dev-tools.sh. The script itself only ever sees fake
# `uv` and `go` binaries that record their arguments. The seam and conformance
# cases use the real uv against pyproject.toml, uv.lock and
# .pre-commit-config.yaml: they run `uv sync --frozen` (creating .venv, and
# downloading on a cold cache) and are skipped without uv, except on CI.
#
# Usage: scripts/install-dev-tools_test.sh

set -uo pipefail

scripts_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${scripts_dir}/.." && pwd)"
script="${scripts_dir}/install-dev-tools.sh"
# A space in the path checks that the script quotes what it is given.
work="$(mktemp -d)/with space"
mkdir -p "${work}"
trap 'rm -rf "$(dirname "${work}")"' EXIT

passed=0
failed=0

# skip MESSAGE : a check that could not run; a failure on CI, where it must run.
skip() {
  if [[ -n "${CI:-}" ]]; then
    fail "$1" "skipped on CI"
  else
    echo "skip - $1"
  fi
}

pass() {
  passed=$((passed + 1))
  echo "ok   - $1"
}
fail() {
  failed=$((failed + 1))
  echo "FAIL - $1" >&2
  if [[ -n "${2:-}" ]]; then echo "       $2" >&2; fi
}

# fake_bin DIR NAME [EXIT_STATUS] : a binary that logs "NAME ARGS..." to ${calls}.
fake_bin() {
  mkdir -p "$1"
  cat >"$1/$2" <<EOF
#!/usr/bin/env bash
echo "$2 \$*" >>"\${calls}"
exit ${3:-0}
EOF
  chmod +x "$1/$2"
}

# Minimal PATH with the tools the script itself needs, but none of uv/go.
base_bin="${work}/base-bin"
mkdir -p "${base_bin}"
for tool in bash env dirname cat; do
  ln -s "$(command -v "${tool}")" "${base_bin}/${tool}"
done

# run BIN_DIR [CWD] : runs the script with only BIN_DIR and base tools on PATH.
run() {
  local bin="$1" cwd="${2:-${repo_root}}"
  calls="${work}/calls"
  : >"${calls}"
  out=$(cd "${cwd}" && calls="${calls}" PATH="${bin}:${base_bin}" "${script}" 2>&1)
  rc=$?
}

# --- Happy path --------------------------------------------------------------

ok_bin="${work}/ok-bin"
fake_bin "${ok_bin}" uv
fake_bin "${ok_bin}" go
run "${ok_bin}"

if [[ "${rc}" == 0 ]]; then
  pass "succeeds when uv and go are available"
else
  fail "succeeds when uv and go are available" "rc=${rc}; output: ${out}"
fi

first_two=$(head -2 "${calls}")
want=$'uv sync --frozen\nuv run --frozen pre-commit install'
if [[ "${first_two}" == "${want}" ]]; then
  pass "syncs the locked tools, then installs the git hook with the locked pre-commit"
else
  fail "syncs the locked tools, then installs the git hook with the locked pre-commit" "got: ${first_two}"
fi

if [[ "$(sed -n '3,$p' "${calls}")" == "go -C ${repo_root}/tools build tool" ]]; then
  pass "builds the Go tools pinned in tools/go.mod after the Python tools"
else
  fail "builds the Go tools pinned in tools/go.mod after the Python tools" "calls: $(cat "${calls}")"
fi

# --- Sad path ----------------------------------------------------------------

no_uv_bin="${work}/no-uv-bin"
fake_bin "${no_uv_bin}" go
run "${no_uv_bin}"
if [[ "${rc}" != 0 ]] && grep -q 'docs.astral.sh/uv' <<<"${out}" && [[ ! -s "${calls}" ]]; then
  pass "fails with an install hint, before installing anything, when uv is missing"
else
  fail "fails with an install hint, before installing anything, when uv is missing" \
    "rc=${rc}; calls: $(cat "${calls}"); output: ${out}"
fi

bad_uv_bin="${work}/bad-uv-bin"
fake_bin "${bad_uv_bin}" uv 3
fake_bin "${bad_uv_bin}" go
run "${bad_uv_bin}"
if [[ "${rc}" != 0 ]] && [[ "$(cat "${calls}")" == "uv sync --frozen" ]]; then
  pass "stops at the first failing step"
else
  fail "stops at the first failing step" "rc=${rc}; calls: $(cat "${calls}")"
fi

no_go_bin="${work}/no-go-bin"
fake_bin "${no_go_bin}" uv
run "${no_go_bin}"
if [[ "${rc}" != 0 ]] && grep -q 'go.dev/doc/install' <<<"${out}" && [[ ! -s "${calls}" ]]; then
  pass "fails with an install hint, before installing anything, when go is missing"
else
  fail "fails with an install hint, before installing anything, when go is missing" \
    "rc=${rc}; calls: $(cat "${calls}"); output: ${out}"
fi

# --- Edge cases --------------------------------------------------------------

run "${ok_bin}" "${repo_root}/pkg"
if [[ "${rc}" == 0 ]] && [[ "$(head -1 "${calls}")" == "uv sync --frozen" ]]; then
  pass "works when started from a subdirectory"
else
  fail "works when started from a subdirectory" "rc=${rc}; output: ${out}"
fi

# --- Seam: the committed lock matches pyproject.toml --------------------------

if command -v uv >/dev/null; then
  if lock_out=$(cd "${repo_root}" && uv lock --check 2>&1); then
    pass "uv.lock is up to date with pyproject.toml"
  else
    fail "uv.lock is up to date with pyproject.toml" "${lock_out}"
  fi
else
  skip "uv not installed; uv.lock consistency not checked"
fi

# A developer's own index (UV_INDEX_URL, uv.toml) must not leak into the lock.
other_sources=$(grep -oE 'registry = "[^"]*"|url = "https://[^/"]*' "${repo_root}/uv.lock" |
  grep -vE 'registry = "https://pypi.org/simple"|url = "https://files.pythonhosted.org' | sort -u)
if [[ -z "${other_sources}" ]]; then
  pass "uv.lock resolves only from PyPI"
else
  fail "uv.lock resolves only from PyPI" "${other_sources}"
fi

# .python-version (what uv installs) must be the requires-python floor, so
# the version everyone runs is the oldest one the lock promises to support.
floor=$(sed -n 's/^requires-python = ">=\([0-9.]*\)"$/\1/p' "${repo_root}/pyproject.toml")
pinned=$(cat "${repo_root}/.python-version" 2>/dev/null)
if [[ -n "${floor}" && "${pinned}" == "${floor}" ]]; then
  pass ".python-version matches the requires-python floor (${floor})"
else
  fail ".python-version matches the requires-python floor" "requires-python >=${floor:-?}, .python-version ${pinned:-missing}"
fi

# --- Conformance: every local hook runs a tool pinned in pyproject.toml -------

# Tools named by `entry: uv run --frozen <tool>` in .pre-commit-config.yaml.
hook_tools=$(sed -n 's/^ *entry: *uv run --frozen \([^ ]*\).*/\1/p' \
  "${repo_root}/.pre-commit-config.yaml" | sort -u)

if [[ "$(wc -w <<<"${hook_tools}")" -lt 4 ]]; then
  fail "local hooks run through uv" "found only: ${hook_tools:-none}"
else
  pass "local hooks run through uv"
fi
# A tool not provided by the locked environment would make `uv run` fall back
# to whatever is on PATH, unpinned.
if command -v uv >/dev/null && (cd "${repo_root}" && uv sync --frozen --quiet); then
  for tool in ${hook_tools}; do
    if [[ -x "${repo_root}/.venv/bin/${tool}" ]]; then
      pass "hook tool ${tool} comes from uv.lock"
    else
      fail "hook tool ${tool} comes from uv.lock" "not in .venv/bin; add its package to pyproject.toml"
    fi
  done
else
  skip "uv not installed or sync failed; hook tools not checked against uv.lock"
fi

# Tools named by `entry: scripts/go-tool.sh <tool>`.
go_hook_tools=$(sed -n 's|^ *entry: *scripts/go-tool.sh \([^ ]*\).*|\1|p' \
  "${repo_root}/.pre-commit-config.yaml" | sort -u)
if [[ "$(wc -w <<<"${go_hook_tools}")" -lt 5 ]]; then
  fail "Go hooks run through go tool" "found only: ${go_hook_tools:-none}"
else
  pass "Go hooks run through go tool"
fi
# The `tool` block of tools/go.mod, one import path per line.
go_tools=$(sed -n '/^tool (/,/^)/p' "${repo_root}/tools/go.mod")
for tool in ${go_hook_tools}; do
  if grep -qE "/${tool}\$" <<<"${go_tools}"; then
    pass "Go hook tool ${tool} is pinned in tools/go.mod"
  else
    fail "Go hook tool ${tool} is pinned in tools/go.mod"
  fi
done

unpinned=$(grep -nE '^ *(entry|repo):' "${repo_root}/.pre-commit-config.yaml" |
  grep -vE 'repo: *local$|entry: *uv run --frozen |entry: *scripts/go-tool.sh ')
if [[ -z "${unpinned}" ]]; then
  pass "every hook is local and runs a pinned tool"
else
  fail "every hook is local and runs a pinned tool" "${unpinned}"
fi

echo
echo "${passed} passed, ${failed} failed"
[[ "${failed}" == 0 ]]
