#!/usr/bin/env bash
#
# Tests for scripts/test-otel-contrib.sh. Runs offline: "contrib" is a local
# bare repo reached through file://, and fake `go` and `make` first on PATH
# record each call (working directory and arguments) instead of building anything.
#
# Usage: scripts/test-otel-contrib_test.sh

set -uo pipefail

script="$(cd "$(dirname "$0")" && pwd)/test-otel-contrib.sh"
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

export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.com
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.com
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

# Fake `go` and `make`: append "<cwd>|<tool> <args>" to CALL_LOG. `make` also
# records the environment it was given, and fails for the target STUB_MAKE_FAIL.
mkdir -p "${work}/bin"
cat >"${work}/bin/go" <<'EOF'
#!/usr/bin/env bash
echo "${PWD}|go $*" >>"${CALL_LOG}"
EOF
cat >"${work}/bin/make" <<'EOF'
#!/usr/bin/env bash
echo "${PWD}|make $*" >>"${CALL_LOG}"
echo "make $*: GOTOOLCHAIN=${GOTOOLCHAIN-} MAKEFLAGS=${MAKEFLAGS-unset} MAKELEVEL=${MAKELEVEL-unset}" >>"${CALL_LOG}.env"
[[ "$1" != "${STUB_MAKE_FAIL:-}" ]] || { echo "fake make: $1 failed" >&2; exit 2; }
EOF
chmod +x "${work}/bin/go" "${work}/bin/make"

# The upstream "contrib" repo, and a seed clone to push new commits from.
upstream="${work}/upstream.git"
seed="${work}/seed"
git init --quiet --bare --initial-branch=main "${upstream}"
git init --quiet --initial-branch=main "${seed}"
mkdir -p "${seed}/exporter/datasetexporter"
echo "module example.com/datasetexporter" >"${seed}/exporter/datasetexporter/go.mod"
echo "v1" >"${seed}/README.md"
git -C "${seed}" add -A
git -C "${seed}" commit --quiet -m one
git -C "${seed}" push --quiet "${upstream}" main
repo_url="file://${upstream}"

# new_tree NAME : a fixture dataset-go root holding the script; prints its path.
new_tree() {
  local dir="${work}/$1"
  mkdir -p "${dir}/scripts"
  cp "${script}" "${dir}/scripts/"
  printf 'module github.com/scalyr/dataset-go\n\ngo 1.24\n' >"${dir}/go.mod"
  echo "${dir}"
}

# run TREE CONTRIB_DIR [ARGS...] : runs the script; sets out, rc, calls (the
# go/make log) and envs (what each make saw).
run() {
  local tree="$1" dir="$2"
  shift 2
  : >"${work}/calls.log"
  : >"${work}/calls.log.env"
  out=$(PATH="${work}/bin:${PATH}" CALL_LOG="${work}/calls.log" CONTRIB_REPO="${CONTRIB_REPO:-${repo_url}}" \
    CONTRIB_DIR="${dir}" "${tree}/scripts/test-otel-contrib.sh" "$@" 2>&1)
  rc=$?
  calls=$(cat "${work}/calls.log")
  envs=$(cat "${work}/calls.log.env")
}

head_of() { git -C "$1" rev-parse HEAD; }

tree=$(new_tree "dataset go")
tree_real="$(cd "${tree}" && pwd -P)"

# --- happy path: fresh clone, then contrib's make targets in the exporter dir ---
dir="${work}/cache dir/contrib"
run "${tree}" "${dir}"
exp="${dir}/exporter/datasetexporter"
expected="${exp}|go mod edit -replace=github.com/scalyr/dataset-go=${tree_real}
${exp}|make tidy
${exp}|make lint
${exp}|make test"
if [[ ${rc} -eq 0 && "${calls}" == "${expected}" ]]; then
  pass "fresh clone runs go mod edit, then make tidy, lint and test in the exporter dir"
else
  fail "fresh clone runs go mod edit, then make tidy, lint and test in the exporter dir" "rc=${rc} out=${out} calls=${calls}"
fi
sha1=$(git -C "${seed}" rev-parse HEAD)
if [[ "$(head_of "${dir}")" == "${sha1}" && "${out}" == *"at ${sha1}"* ]]; then
  pass "the clone is at upstream main and its SHA is printed"
else
  fail "the clone is at upstream main and its SHA is printed" "out=${out}"
fi

# --- reuse: a second run fetches the new main and discards local changes ---
echo "v2" >"${seed}/README.md"
git -C "${seed}" commit --quiet -am two
git -C "${seed}" push --quiet "${upstream}" main
sha2=$(git -C "${seed}" rev-parse HEAD)
echo "replace stale => ../stale" >>"${exp}/go.mod"
echo "junk" >"${dir}/untracked.txt"
mkdir -p "${dir}/ignored-by-nothing" && echo x >"${dir}/ignored-by-nothing/f"
run "${tree}" "${dir}"
if [[ ${rc} -eq 0 && "${out}" == *Updating* && "${out}" != *Cloning* && "$(head_of "${dir}")" == "${sha2}" ]]; then
  pass "a second run reuses the clone and moves it to the latest main"
else
  fail "a second run reuses the clone and moves it to the latest main" "rc=${rc} out=${out}"
fi
if [[ "$(cat "${exp}/go.mod")" == "module example.com/datasetexporter" && ! -e "${dir}/untracked.txt" &&
  ! -e "${dir}/ignored-by-nothing" && -z "$(git -C "${dir}" status --porcelain)" ]]; then
  pass "local edits and untracked files are discarded before testing"
else
  fail "local edits and untracked files are discarded before testing" "status=$(git -C "${dir}" status --porcelain)"
fi

# --- seam: a url.insteadOf rewrite (e.g. https -> ssh) does not break reuse ---
# CONTRIB_REPO is an https URL that git rewrites to the file:// upstream.
rewritten="${work}/rewritten"
insteadof() {
  GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="url.file://${work}/.insteadOf" \
    GIT_CONFIG_VALUE_0="https://contrib.invalid/" CONTRIB_REPO="https://contrib.invalid/upstream.git" "$@"
}
insteadof run "${tree}" "${rewritten}"
rc1=${rc}
insteadof run "${tree}" "${rewritten}"
if [[ ${rc1} -eq 0 && ${rc} -eq 0 && "${out}" == *Updating* ]]; then
  pass "a clone whose URL git rewrites with insteadOf is reused"
else
  fail "a clone whose URL git rewrites with insteadOf is reused" "rc1=${rc1} rc=${rc} out=${out}"
fi

# --- sad: a clone of another repo is refused and left untouched ---
other="${work}/other"
git init --quiet --initial-branch=main "${other}"
git -C "${other}" remote add origin https://example.com/someone/else.git
echo "precious" >"${other}/work.txt"
run "${tree}" "${other}"
if [[ ${rc} -ne 0 && "${out}" == *"refusing to reset"* && "$(cat "${other}/work.txt")" == precious && -z "${calls}" ]]; then
  pass "refuses a clone whose origin is not CONTRIB_REPO"
else
  fail "refuses a clone whose origin is not CONTRIB_REPO" "rc=${rc} out=${out}"
fi

# --- sad: a non-empty directory that is not a clone is refused ---
plain="${work}/plain"
mkdir -p "${plain}" && echo "precious" >"${plain}/work.txt"
run "${tree}" "${plain}"
if [[ ${rc} -ne 0 && "${out}" == *"not a git clone"* && -f "${plain}/work.txt" && -z "${calls}" ]]; then
  pass "refuses a directory that is not a git clone"
else
  fail "refuses a directory that is not a git clone" "rc=${rc} out=${out}"
fi

# --- sad: a subdirectory of another repo's work tree is refused ---
sub="${other}/sub"
mkdir -p "${sub}" && echo x >"${sub}/f"
CONTRIB_REPO=https://example.com/someone/else.git run "${tree}" "${sub}"
if [[ ${rc} -ne 0 && "${out}" == *"not a git clone"* && "$(cat "${other}/work.txt")" == precious ]]; then
  pass "refuses a subdirectory of another repo's work tree"
else
  fail "refuses a subdirectory of another repo's work tree" "rc=${rc} out=${out}"
fi

# --- sad: a file at CONTRIB_DIR is refused ---
echo x >"${work}/a-file"
run "${tree}" "${work}/a-file"
if [[ ${rc} -ne 0 && "${out}" == *"not a directory"* ]]; then
  pass "refuses a CONTRIB_DIR that is a file"
else
  fail "refuses a CONTRIB_DIR that is a file" "rc=${rc} out=${out}"
fi

# --- sad: a failing contrib target fails the script and stops later ones ---
STUB_MAKE_FAIL=lint run "${tree}" "${dir}"
if [[ ${rc} -ne 0 && "${out}" == *"lint failed"* && "${calls}" == *"|make lint" && "${calls}" != *"make test"* ]]; then
  pass "a failing make lint fails the script and make test does not run"
else
  fail "a failing make lint fails the script and make test does not run" "rc=${rc} out=${out} calls=${calls}"
fi
STUB_MAKE_FAIL="test" run "${tree}" "${dir}"
if [[ ${rc} -ne 0 && "${calls}" == *"|make test" ]]; then
  pass "a failing make test fails the script"
else
  fail "a failing make test fails the script" "rc=${rc} out=${out}"
fi
STUB_MAKE_FAIL=tidy run "${tree}" "${dir}"
if [[ ${rc} -ne 0 && "${calls}" != *"make lint"* ]]; then
  pass "a failing make tidy fails the script before any check"
else
  fail "a failing make tidy fails the script before any check" "rc=${rc} calls=${calls}"
fi

# --- edge: CONTRIB_TARGETS picks the checks, tidy always runs first ---
CONTRIB_TARGETS="test-twice" run "${tree}" "${dir}"
if [[ ${rc} -eq 0 && "$(grep -o 'make .*' <<<"${calls}")" == "make tidy
make test-twice" ]]; then
  pass "CONTRIB_TARGETS replaces the default checks, after make tidy"
else
  fail "CONTRIB_TARGETS replaces the default checks, after make tidy" "rc=${rc} calls=${calls}"
fi

# --- sad: an empty CONTRIB_TARGETS is rejected rather than passing vacuously ---
CONTRIB_TARGETS="  " run "${tree}" "${dir}"
if [[ ${rc} -ne 0 && "${out}" == *"CONTRIB_TARGETS is empty"* && -z "${calls}" ]]; then
  pass "rejects an empty CONTRIB_TARGETS"
else
  fail "rejects an empty CONTRIB_TARGETS" "rc=${rc} out=${out} calls=${calls}"
fi

# --- seam: our make's flags and variables do not leak into contrib's make ---
MAKEFLAGS="-j4 -- COUNT=5" MAKELEVEL=1 MFLAGS=-j4 run "${tree}" "${dir}"
if [[ ${rc} -eq 0 && "${envs}" == *"MAKEFLAGS=unset MAKELEVEL=unset"* && "${envs}" != *COUNT* ]]; then
  pass "MAKEFLAGS and MAKELEVEL from a parent make are not passed on"
else
  fail "MAKEFLAGS and MAKELEVEL from a parent make are not passed on" "envs=${envs}"
fi

# --- sad: an unreachable upstream fails before any go call ---
CONTRIB_REPO="file://${work}/missing.git" run "${tree}" "${work}/never-cloned"
if [[ ${rc} -ne 0 && -z "${calls}" ]]; then
  pass "an unreachable CONTRIB_REPO fails the script"
else
  fail "an unreachable CONTRIB_REPO fails the script" "rc=${rc} out=${out}"
fi

# --- sad: upstream without the exporter fails with a clear error ---
git -C "${seed}" rm --quiet -r exporter
git -C "${seed}" commit --quiet -m "drop exporter"
git -C "${seed}" push --quiet "${upstream}" main
run "${tree}" "${dir}"
if [[ ${rc} -ne 0 && "${out}" == *"exporter/datasetexporter/go.mod not found"* && -z "${calls}" ]]; then
  pass "a contrib without exporter/datasetexporter fails clearly"
else
  fail "a contrib without exporter/datasetexporter fails clearly" "rc=${rc} out=${out}"
fi
git -C "${seed}" revert --quiet --no-edit HEAD >/dev/null
git -C "${seed}" push --quiet "${upstream}" main

# --- sad: arguments are rejected ---
run "${tree}" "${dir}" extra
if [[ ${rc} -eq 2 && "${out}" == *Usage* && -z "${calls}" ]]; then
  pass "rejects arguments"
else
  fail "rejects arguments" "rc=${rc} out=${out}"
fi

# --- sad: a go.mod without a module line is rejected ---
bad=$(new_tree no-module)
echo "go 1.24" >"${bad}/go.mod"
run "${bad}" "${dir}"
if [[ ${rc} -ne 0 && "${out}" == *"no module line"* && -z "${calls}" ]]; then
  pass "rejects a dataset-go root without a module line"
else
  fail "rejects a dataset-go root without a module line" "rc=${rc} out=${out}"
fi

# --- edge: an empty pre-created CONTRIB_DIR is cloned into ---
empty="${work}/empty"
mkdir -p "${empty}"
run "${tree}" "${empty}"
if [[ ${rc} -eq 0 && "${out}" == *Cloning* && -f "${empty}/exporter/datasetexporter/go.mod" ]]; then
  pass "an empty CONTRIB_DIR is cloned into"
else
  fail "an empty CONTRIB_DIR is cloned into" "rc=${rc} out=${out}"
fi

# --- edge: GOTOOLCHAIN defaults to auto, an explicit value is kept ---
run "${tree}" "${dir}"
if [[ ${rc} -eq 0 && "${envs}" == *"make test: GOTOOLCHAIN=auto"* ]]; then
  pass "GOTOOLCHAIN defaults to auto"
else
  fail "GOTOOLCHAIN defaults to auto" "rc=${rc} envs=${envs}"
fi
GOTOOLCHAIN=local run "${tree}" "${dir}"
if [[ ${rc} -eq 0 && "${envs}" == *"GOTOOLCHAIN=local"* && "${envs}" != *"GOTOOLCHAIN=auto"* ]]; then
  pass "an explicit GOTOOLCHAIN is kept"
else
  fail "an explicit GOTOOLCHAIN is kept" "rc=${rc} envs=${envs}"
fi

# --- conformance: the replace target is absolute and declares the module ---
replace=$(grep -o 'go mod edit -replace=.*' <<<"${calls}" | head -1)
target="${replace#*-replace=github.com/scalyr/dataset-go=}"
if [[ "${target}" == /* ]] && grep -qx 'module github.com/scalyr/dataset-go' "${target}/go.mod"; then
  pass "the replace target is an absolute path whose go.mod declares the module"
else
  fail "the replace target is an absolute path whose go.mod declares the module" "replace=${replace}"
fi

# --- conformance: the real repo's module path is what the script replaces ---
repo="$(cd "$(dirname "$0")/.." && pwd)"
if grep -qx 'module github.com/scalyr/dataset-go' "${repo}/go.mod"; then
  pass "the repo's go.mod declares github.com/scalyr/dataset-go"
else
  fail "the repo's go.mod declares github.com/scalyr/dataset-go"
fi

echo
echo "${passed} passed, ${failed} failed"
[[ ${failed} -eq 0 ]]
