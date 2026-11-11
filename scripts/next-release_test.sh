#!/usr/bin/env bash
#
# Tests for scripts/next-release.sh. Builds throwaway git repos; most cases pin
# the runtime module set with RUNTIME_MODULES, and the seam cases run the real
# `go list` against a copy of this module (needs the Go module cache or network).
#
# Usage: scripts/next-release_test.sh

set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
script="${here}/next-release.sh"
real_root="$(cd "${here}/.." && pwd)"
work=$(mktemp -d)
trap 'rm -rf "${work}"' EXIT

# Isolate from the user's git config (signing, hooks, default branch, ...).
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.com
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.com

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

GOMOD='module example.com/fixture

go 1.26.0

require (
	github.com/stretchr/testify v1.12.0
	go.uber.org/zap v1.27.0
)'

# new_repo NAME [TAG] : fixture repo with one commit tagged TAG (default
# v0.21.0; "-" for no tag). Prints its path.
new_repo() {
  local dir="${work}/$1" tag="${2:-v0.21.0}"
  mkdir -p "${dir}/pkg/lib"
  git -C "${dir}" init -q -b main
  echo "${GOMOD}" >"${dir}/go.mod"
  printf 'package lib\n' >"${dir}/pkg/lib/lib.go"
  commit "${dir}" initial
  if [[ "${tag}" != "-" ]]; then git -C "${dir}" tag -a "${tag}" -m "${tag}"; fi
  echo "${dir}"
}

# commit DIR MSG : commits everything in DIR (empty commits allowed).
commit() {
  git -C "$1" add -A && git -C "$1" commit -q --allow-empty -m "$2"
}

# write DIR PATH CONTENT : creates or overwrites DIR/PATH.
write() {
  mkdir -p "$(dirname "$1/$2")"
  printf '%s\n' "$3" >"$1/$2"
}

# run DIR REV [VAR=VALUE...] : runs the script for REV. RUNTIME_MODULES
# defaults to "go.uber.org/zap" unless overridden; pass RUNTIME_MODULES=-unset
# to run the real go list. stdout in ${out}, stderr in ${err}, status in ${rc}.
run() {
  local dir="$1" rev="$2"
  shift 2
  local -a envs=("RUNTIME_MODULES=go.uber.org/zap")
  local kv
  for kv in "$@"; do
    if [[ "${kv}" == "RUNTIME_MODULES=-unset" ]]; then envs=(-u RUNTIME_MODULES); else envs+=("${kv}"); fi
  done
  out=$(cd "${dir}" && env "${envs[@]}" REPO_ROOT="${dir}" "${script}" "${rev}" 2>"${work}/stderr")
  rc=$?
  err=$(cat "${work}/stderr")
}

# expect NAME RELEASE BASE TAG REASON : the last run succeeded with exactly this
# output. A release must also be base patch + 1 (conformance with the
# documented contract of the script).
expect() {
  local want="release=$2"$'\n'"base=$3"$'\n'"tag=$4"$'\n'"reason=$5"
  if [[ "${rc}" != 0 || "${out}" != "${want}" ]]; then
    fail "$1" "rc=${rc}; got '${out//$'\n'/ }', want '${want//$'\n'/ }'; stderr: ${err}"
    return
  fi
  if [[ "$2" == true ]]; then
    local b="${3#v}" t="${4#v}"
    if ! [[ "$4" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
      [[ "${b%.*}" != "${t%.*}" || "${t##*.}" != "$((${b##*.} + 1))" ]]; then
      fail "$1" "tag $4 is not the next patch of $3"
      return
    fi
  fi
  pass "$1"
}

# expect_fail NAME PATTERN : the last run failed, printed nothing on stdout,
# and stderr matches PATTERN.
expect_fail() {
  if [[ "${rc}" != 0 && -z "${out}" ]] && grep -qE "$2" <<<"${err}"; then
    pass "$1"
  else
    fail "$1" "rc=${rc}, stdout='${out}', wanted failure matching /$2/; stderr: ${err}"
  fi
}

# --- Happy path --------------------------------------------------------------

repo=$(new_repo code)
write "${repo}" pkg/lib/lib.go $'package lib\n\nconst X = 1'
commit "${repo}" "feat: change"
run "${repo}" HEAD
expect "non-test code change in pkg/ releases a patch" true v0.21.0 v0.21.1 code

repo=$(new_repo internal)
write "${repo}" internal/pkg/util/util.go "package util"
commit "${repo}" "feat: internal"
run "${repo}" HEAD
expect "new file under internal/ releases" true v0.21.0 v0.21.1 code

repo=$(new_repo deleted)
git -C "${repo}" rm -q pkg/lib/lib.go
commit "${repo}" "refactor: delete"
run "${repo}" HEAD
expect "deleting a non-test file releases" true v0.21.0 v0.21.1 code

repo=$(new_repo dep)
sed -i.bak 's#go.uber.org/zap v1.27.0#go.uber.org/zap v1.28.0#' "${repo}/go.mod" && rm "${repo}/go.mod.bak"
commit "${repo}" "chore(deps): zap"
run "${repo}" HEAD
expect "runtime dependency bump releases" true v0.21.0 v0.21.1 deps:go.uber.org/zap
run "${repo}" HEAD "RUNTIME_MODULES=go.uber.org/zap"$'\n'"github.com/stretchr/testify"
expect "runtime set may list unchanged modules" true v0.21.0 v0.21.1 deps:go.uber.org/zap

repo=$(new_repo deps-two)
sed -i.bak -e 's#zap v1.27.0#zap v1.28.0#' -e 's#testify v1.12.0#testify v1.12.1#' "${repo}/go.mod" && rm "${repo}/go.mod.bak"
commit "${repo}" "chore(deps): both"
run "${repo}" HEAD "RUNTIME_MODULES=go.uber.org/zap"$'\n'"github.com/stretchr/testify"
expect "several runtime modules are listed, sorted" true v0.21.0 v0.21.1 deps:github.com/stretchr/testify,go.uber.org/zap

repo=$(new_repo dep-added)
(cd "${repo}" && go mod edit -require=go.opentelemetry.io/otel@v1.47.0)
commit "${repo}" "chore(deps): add otel"
run "${repo}" HEAD "RUNTIME_MODULES=go.opentelemetry.io/otel"
expect "added runtime requirement releases" true v0.21.0 v0.21.1 deps:go.opentelemetry.io/otel

repo=$(new_repo go-directive)
(cd "${repo}" && go mod edit -go=1.27.0)
commit "${repo}" "chore: go 1.27"
run "${repo}" HEAD
expect "go directive bump releases" true v0.21.0 v0.21.1 go-directive

repo=$(new_repo toolchain)
(cd "${repo}" && go mod edit -toolchain=go1.26.3)
commit "${repo}" "chore: toolchain"
run "${repo}" HEAD
expect "toolchain directive added releases" true v0.21.0 v0.21.1 go-directive

repo=$(new_repo by-branch)
write "${repo}" pkg/lib/b.go "package lib"
commit "${repo}" "feat: b"
run "${repo}" main
expect "revision may be a branch name" true v0.21.0 v0.21.1 code

# --- Sad path ----------------------------------------------------------------

repo=$(new_repo args)
run "${repo}" ""
expect_fail "empty revision is rejected" "usage"
out=$(REPO_ROOT="${repo}" "${script}" 2>"${work}/stderr")
rc=$?
err=$(cat "${work}/stderr")
expect_fail "missing revision is rejected" "usage"
out=$(REPO_ROOT="${repo}" "${script}" HEAD HEAD 2>"${work}/stderr")
rc=$?
err=$(cat "${work}/stderr")
expect_fail "extra arguments are rejected" "usage"

run "${repo}" deadbeefdeadbeefdeadbeefdeadbeefdeadbeef
expect_fail "unknown commit is rejected" "unknown commit"

repo=$(new_repo no-tags -)
run "${repo}" HEAD
expect_fail "repo without tags fails" "no release tag"

repo=$(new_repo bad-tags -)
for t in v1.2 v1.2.3-rc1 foo 1.2.3 v01.2.3 v1.2.3.4; do git -C "${repo}" tag "${t}"; done
run "${repo}" HEAD
expect_fail "only malformed tags count as no tags" "no release tag"

repo=$(new_repo diverged)
git -C "${repo}" checkout -q -b side
write "${repo}" pkg/lib/side.go "package lib"
commit "${repo}" side
git -C "${repo}" tag -a v0.22.0 -m v0.22.0
git -C "${repo}" checkout -q main
write "${repo}" pkg/lib/main.go "package lib"
commit "${repo}" main
run "${repo}" main
expect_fail "latest tag off main's history fails" "not an ancestor"

repo=$(new_repo bad-gomod)
write "${repo}" go.mod "this is not a go.mod"
commit "${repo}" "broken"
run "${repo}" HEAD
expect_fail "unparseable go.mod fails" "cannot parse go.mod"

# The tagged base already imports a module go.mod does not require, so the
# only change is go.mod and the deps rule must run go list, which exits non-zero.
repo=$(new_repo golist-fails -)
mkdir -p "${repo}/pkg/broken"
printf 'package broken\n\nimport _ "example.com/missing/pkg"\n' >"${repo}/pkg/broken/broken.go"
commit "${repo}" "broken import"
git -C "${repo}" tag -a v0.21.0 -m v0.21.0
sed -i.bak 's#zap v1.27.0#zap v1.28.0#' "${repo}/go.mod" && rm "${repo}/go.mod.bak"
commit "${repo}" "deps"
run "${repo}" HEAD RUNTIME_MODULES=-unset GOPROXY=off
expect_fail "go list failure is reported, not treated as no deps" "go list failed"

# --- Edge cases --------------------------------------------------------------

repo=$(new_repo non-release)
for path in pkg/lib/lib_test.go pkg/lib/testdata/fixture.go internal/x/testdata/a.go \
  examples/client/main.go test/x.go scripts/x.sh .github/workflows/x.yaml \
  README.md docs/x.md Makefile go.sum pkg/lib/notes.txt; do
  write "${repo}" "${path}" "content of ${path}"
done
commit "${repo}" "chore: everything but code"
run "${repo}" HEAD
expect "tests, testdata, examples, tooling, docs, go.sum never release" false v0.21.0 "" none

repo=$(new_repo testify)
sed -i.bak 's#testify v1.12.0#testify v1.12.1#' "${repo}/go.mod" && rm "${repo}/go.mod.bak"
commit "${repo}" "chore(deps): testify"
run "${repo}" HEAD
expect "test-only dependency bump does not release" false v0.21.0 "" none
run "${repo}" HEAD "RUNTIME_MODULES="
expect "empty runtime set never releases on deps" false v0.21.0 "" none

repo=$(new_repo replace-only)
(cd "${repo}" && go mod edit -replace=go.uber.org/zap=../zap)
commit "${repo}" "local replace"
run "${repo}" HEAD
expect "replace directive alone does not release" false v0.21.0 "" none

repo=$(new_repo empty)
commit "${repo}" "empty"
run "${repo}" HEAD
expect "empty diff does not release" false v0.21.0 "" none

repo=$(new_repo at-tag)
run "${repo}" v0.21.0
expect "commit equal to the tag does not release" false v0.21.0 "" none
old=$(git -C "${repo}" rev-parse HEAD)
write "${repo}" pkg/lib/next.go "package lib"
commit "${repo}" next
git -C "${repo}" tag -a v0.21.1 -m v0.21.1
run "${repo}" "${old}"
expect "commit older than the tag does not release" false v0.21.1 "" none

repo=$(new_repo numeric v0.9.9)
git -C "${repo}" tag -a v0.10.0 -m v0.10.0
write "${repo}" pkg/lib/n.go "package lib"
commit "${repo}" n
run "${repo}" HEAD
expect "tags sort numerically, not lexically" true v0.10.0 v0.10.1 code

repo=$(new_repo patch-rollover v0.9.9)
git -C "${repo}" tag v1.0.0-rc1
write "${repo}" pkg/lib/n.go "package lib"
commit "${repo}" n
run "${repo}" HEAD
expect "patch 9 becomes 10 and pre-release tags are ignored" true v0.9.9 v0.9.10 code

repo=$(new_repo multi-commit)
write "${repo}" pkg/lib/a.go "package lib"
commit "${repo}" a
write "${repo}" README.md "docs"
commit "${repo}" docs
run "${repo}" HEAD
expect "code change in an earlier untagged commit still releases" true v0.21.0 v0.21.1 code

# --- Conformance: the release commit does not trigger another release ---------

repo=$(new_repo no-loop)
write "${repo}" pkg/lib/lib.go $'package lib\n\nconst Y = 2'
commit "${repo}" "feat: y"
write "${repo}" pkg/version/version.go $'package version\n\nconst Version = "0.21.1"'
commit "${repo}" "chore(release): v0.21.1"
git -C "${repo}" tag -a v0.21.1 -m v0.21.1
run "${repo}" HEAD
expect "release commit tagged with its own version does not re-release" false v0.21.1 "" none

# --- Seam: real `go list` on a copy of this module ----------------------------

# seam_repo NAME MODULE : copy of the real module whose tagged base requires
# MODULE at a fake older version; HEAD restores the real go.mod.
seam_repo() {
  local dir="${work}/$1"
  mkdir -p "${dir}"
  cp -R "${real_root}/go.mod" "${real_root}/go.sum" "${real_root}/pkg" "${real_root}/internal" "${dir}/"
  git -C "${dir}" init -q -b main
  (cd "${dir}" && go mod edit -require="$2@v0.0.1")
  commit "${dir}" base
  git -C "${dir}" tag -a v0.21.0 -m v0.21.0
  cp "${real_root}/go.mod" "${dir}/go.mod"
  commit "${dir}" "bump $2"
  echo "${dir}"
}

repo=$(seam_repo seam-zap go.uber.org/zap)
run "${repo}" HEAD RUNTIME_MODULES=-unset
expect "real go list: bump of a linked module releases" true v0.21.0 v0.21.1 deps:go.uber.org/zap

repo=$(seam_repo seam-testify github.com/stretchr/testify)
run "${repo}" HEAD RUNTIME_MODULES=-unset
expect "real go list: test-only module does not release" false v0.21.0 "" none

# Checked out elsewhere: go list must still see <sha>, via a temporary worktree.
repo=$(seam_repo seam-worktree go.uber.org/zap)
target=$(git -C "${repo}" rev-parse HEAD)
git -C "${repo}" checkout -q --detach v0.21.0
run "${repo}" "${target}" RUNTIME_MODULES=-unset
expect "real go list runs at <sha> when HEAD is elsewhere" true v0.21.0 v0.21.1 deps:go.uber.org/zap
if [[ "$(git -C "${repo}" worktree list | wc -l | tr -d ' ')" == 1 ]]; then
  pass "temporary worktree is removed"
else
  fail "temporary worktree is removed" "$(git -C "${repo}" worktree list)"
fi

# The workflow appends stdout to $GITHUB_OUTPUT and reads these keys.
keys=$(sed -E 's/=.*//' <<<"${out}" | paste -sd' ' -)
if [[ "${keys}" == "release base tag reason" ]] && ! grep -qvE '^[a-z]+=[^[:space:]]*$' <<<"${out}"; then
  pass "output is key=value lines with the keys the workflow reads"
else
  fail "output is key=value lines with the keys the workflow reads" "got: ${out}"
fi

echo
echo "${passed} passed, ${failed} failed"
[[ "${failed}" == 0 ]]
