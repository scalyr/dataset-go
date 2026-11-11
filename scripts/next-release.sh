#!/usr/bin/env bash
#
# Decides whether commit <sha> needs a new patch release, i.e. whether anything
# a consumer of the library gets changed since the latest release tag:
#
#   code         a non-test .go file under pkg/ or internal/ changed
#   go-directive the `go` or `toolchain` directive in the root go.mod changed
#   deps:<mods>  a root go.mod requirement changed for a module that the
#                library's non-test packages actually link
#                (`go list -deps ./...`), so testify-only bumps do not count
#
# Everything else (tests, testdata, examples/, scripts/, .github/, docs, go.sum
# on its own) never releases. A removed requirement needs no check of its own:
# dropping a linked module requires a code change, which `code` catches.
# Replace/exclude directives are ignored: they do not apply to consumers.
#
# The diff is always taken from the latest tag, not from the triggering commit,
# so a skipped or failed run is caught up by the next one, and the release
# commit itself (which the tag points at) yields release=false.
#
# Usage:
#   scripts/next-release.sh <sha>   print release=, base=, tag=, reason= lines,
#                                   ready to append to $GITHUB_OUTPUT
#
# Environment (mainly for tests):
#   REPO_ROOT        repository root (default: parent of this script's directory)
#   RUNTIME_MODULES  newline-separated module paths to use instead of running
#                    `go list` (set, even to empty, to skip go list entirely)

set -euo pipefail
export LC_ALL=C # comm needs the same collation as sort

repo_root=${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

tmp=$(mktemp -d)
worktree=""
cleanup() {
  if [[ -n "${worktree}" ]]; then git -C "${repo_root}" worktree remove --force "${worktree}" >/dev/null 2>&1 || true; fi
  rm -rf "${tmp}"
}
trap cleanup EXIT

[[ $# -eq 1 && -n "$1" ]] || die "usage: $0 <sha>"
cd "${repo_root}"
sha=$(git rev-parse --verify --quiet "$1^{commit}") || die "unknown commit '$1'"

# Semver release tags only; no leading zeros, no pre-release/build suffixes.
semver_re='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
base=$(git tag --list 'v*' | grep -E "${semver_re}" | sort -V | tail -n1 || true)
[[ -n "${base}" ]] || die "no release tag vX.Y.Z found (in CI, check out with fetch-depth: 0)"
base_sha=$(git rev-parse "${base}^{commit}")

emit() {
  echo "release=$1"
  echo "base=${base}"
  echo "tag=$2"
  echo "reason=$3"
}

# Already released (a re-run, or a run for an older commit that lost the race).
if git merge-base --is-ancestor "${sha}" "${base_sha}"; then
  emit false "" none
  exit 0
fi
git merge-base --is-ancestor "${base_sha}" "${sha}" ||
  die "latest tag ${base} is not an ancestor of ${sha}; tags and main have diverged"

IFS=. read -r major minor patch <<<"${base#v}"
next="v${major}.${minor}.$((patch + 1))"

changed=$(git diff --name-only "${base_sha}" "${sha}")

# No `grep -q` here: it exits early, and the SIGPIPE upstream fails the pipe.
code=$(grep -E '^(pkg|internal)/.*\.go$' <<<"${changed}" |
  grep -Ev '_test\.go$' | grep -Ev '(^|/)testdata/' || true)
if [[ -n "${code}" ]]; then
  emit true "${next}" code
  exit 0
fi

if ! grep -qx 'go.mod' <<<"${changed}"; then
  emit false "" none
  exit 0
fi

# go.mod as JSON at a commit; an absent go.mod reads as an empty module.
gomod_json() {
  if git show "$1:go.mod" >"${tmp}/$2.mod" 2>/dev/null; then
    go mod edit -json "${tmp}/$2.mod" || die "cannot parse go.mod at $1"
  else
    echo '{}'
  fi
}
old=$(gomod_json "${base_sha}" old)
new=$(gomod_json "${sha}" new)

directive='"\(.Go // "") \(.Toolchain.Name // "")"'
if [[ "$(jq -r "${directive}" <<<"${old}")" != "$(jq -r "${directive}" <<<"${new}")" ]]; then
  emit true "${next}" go-directive
  exit 0
fi

requires='.Require[]? | "\(.Path) \(.Version)"'
jq -r "${requires}" <<<"${old}" | sort -u >"${tmp}/old.req"
jq -r "${requires}" <<<"${new}" | sort -u >"${tmp}/new.req"
comm -13 "${tmp}/old.req" "${tmp}/new.req" | awk '{print $1}' | sort -u >"${tmp}/changed.mods"

if [[ -n "${RUNTIME_MODULES+set}" ]]; then
  printf '%s\n' "${RUNTIME_MODULES}" >"${tmp}/runtime.mods"
else
  # go list must see the tree at <sha>, not whatever is checked out.
  dir="${repo_root}"
  if [[ "$(git rev-parse HEAD)" != "${sha}" ]]; then
    worktree="${tmp}/worktree"
    git worktree add --detach --quiet "${worktree}" "${sha}" || die "cannot check out ${sha}"
    dir="${worktree}"
  fi
  (cd "${dir}" && GOFLAGS=-mod=readonly go list -deps -f '{{with .Module}}{{.Path}}{{end}}' ./...) \
    >"${tmp}/runtime.mods" || die "go list failed at ${sha}"
fi
sort -u -o "${tmp}/runtime.mods" "${tmp}/runtime.mods"

mods=$(comm -12 "${tmp}/changed.mods" "${tmp}/runtime.mods" | grep . | paste -sd, - || true)
if [[ -n "${mods}" ]]; then
  emit true "${next}" "deps:${mods}"
else
  emit false "" none
fi
