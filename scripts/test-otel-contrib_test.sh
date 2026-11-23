#!/usr/bin/env bash
#
# Tests for scripts/test-otel-contrib.sh. Runs offline: "contrib" is a local
# bare repo reached through file://, and a fake `go` first on PATH records each
# call (working directory and arguments) instead of building anything.
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

# Fake `go`: appends "<cwd>|<args>" to GO_LOG; `go test` exits STUB_GO_EXIT.
mkdir -p "${work}/bin"
cat >"${work}/bin/go" <<'EOF'
#!/usr/bin/env bash
echo "${PWD}|$*" >>"${GO_LOG}"
if [[ "$1" == test ]]; then exit "${STUB_GO_EXIT:-0}"; fi
EOF
chmod +x "${work}/bin/go"

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

# run TREE CONTRIB_DIR [ARGS...] : runs the script; sets out, rc and calls (the go log).
run() {
  local tree="$1" dir="$2"
  shift 2
  : >"${work}/go.log"
  out=$(PATH="${work}/bin:${PATH}" GO_LOG="${work}/go.log" CONTRIB_REPO="${CONTRIB_REPO:-${repo_url}}" \
    CONTRIB_DIR="${dir}" "${tree}/scripts/test-otel-contrib.sh" "$@" 2>&1)
  rc=$?
  calls=$(cat "${work}/go.log")
}

head_of() { git -C "$1" rev-parse HEAD; }

tree=$(new_tree "dataset go")
tree_real="$(cd "${tree}" && pwd -P)"

# --- happy path: fresh clone, go called in the exporter dir, in order ---
dir="${work}/cache dir/contrib"
run "${tree}" "${dir}"
exp="${dir}/exporter/datasetexporter"
expected="${exp}|mod edit -replace=github.com/scalyr/dataset-go=${tree_real}
${exp}|mod tidy
${exp}|build ./...
${exp}|test -race -count=1 ./..."
if [[ ${rc} -eq 0 && "${calls}" == "${expected}" ]]; then
  pass "fresh clone runs mod edit, tidy, build and test in the exporter dir"
else
  fail "fresh clone runs mod edit, tidy, build and test in the exporter dir" "rc=${rc} out=${out} calls=${calls}"
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

# --- sad: failing contrib tests fail the script ---
STUB_GO_EXIT=1 run "${tree}" "${dir}"
if [[ ${rc} -ne 0 && "${calls}" == *"|test "* ]]; then
  pass "a failing go test fails the script"
else
  fail "a failing go test fails the script" "rc=${rc} out=${out}"
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

# --- edge: GOTEST_FLAGS reaches go test, GOTOOLCHAIN defaults to auto ---
cat >"${work}/bin/go" <<'EOF'
#!/usr/bin/env bash
echo "${PWD}|$*|GOTOOLCHAIN=${GOTOOLCHAIN}" >>"${GO_LOG}"
EOF
GOTEST_FLAGS="-run TestX -v" run "${tree}" "${dir}"
if [[ ${rc} -eq 0 && "${calls}" == *"|test -run TestX -v ./...|GOTOOLCHAIN=auto"* ]]; then
  pass "GOTEST_FLAGS reaches go test and GOTOOLCHAIN defaults to auto"
else
  fail "GOTEST_FLAGS reaches go test and GOTOOLCHAIN defaults to auto" "rc=${rc} calls=${calls}"
fi
GOTOOLCHAIN=local run "${tree}" "${dir}"
if [[ ${rc} -eq 0 && "${calls}" == *"GOTOOLCHAIN=local"* && "${calls}" != *"GOTOOLCHAIN=auto"* ]]; then
  pass "an explicit GOTOOLCHAIN is kept"
else
  fail "an explicit GOTOOLCHAIN is kept" "rc=${rc} calls=${calls}"
fi

# --- conformance: the replace target is absolute and declares the module ---
replace=$(grep -o 'mod edit -replace=[^|]*' <<<"${calls}" | head -1)
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
