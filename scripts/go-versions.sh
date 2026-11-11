#!/usr/bin/env bash
#
# Prints the Go versions CI should use, computed at run time so they cannot go
# stale (no generated file to keep in sync with go.mod):
#
#   floor   = <major>.<minor> of the `go` directive in the root go.mod
#   matrix  = floor + every currently supported Go minor that is >= floor
#   primary = highest entry of matrix (used by single-version jobs)
#
# Versions below the floor are never in the matrix: with GOTOOLCHAIN=local
# (what actions/setup-go sets) they cannot build the module at all.
#
# Supported releases come from https://go.dev/dl/?mode=json, which lists only
# the currently supported stable releases (the latest two minors). A new Go
# release therefore enters the matrix on the next CI run, nightly at the latest.
#
# Usage:
#   scripts/go-versions.sh           print `primary=...` and `matrix=[...]` lines,
#                                    ready to append to $GITHUB_OUTPUT
#   scripts/go-versions.sh --json    print {"matrix": [...], "primary": "..."}
#
# Environment (mainly for tests):
#   REPO_ROOT    repository root (default: parent of this script's directory)
#   GO_DL_JSON   read the release list from this file instead of go.dev
#   GO_DL_URL    release list URL (default: https://go.dev/dl/?mode=json)

set -euo pipefail

repo_root=${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}
gomod="${repo_root}/go.mod"
dl_url=${GO_DL_URL:-https://go.dev/dl/?mode=json}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

# version_ge A B: true when version A >= version B (dotted numeric).
version_ge() {
  [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n1)" == "$2" ]]
}

format=output
for arg in "$@"; do
  case "${arg}" in
    --json) format=json ;;
    *) die "unknown argument '${arg}' (expected --json)" ;;
  esac
done

[[ -f "${gomod}" ]] || die "${gomod} not found"
full=$(grep -E '^go [0-9]+\.[0-9]+' "${gomod}" | head -n1 | awk '{print $2}' || true)
[[ -n "${full}" ]] || die "could not parse go version from ${gomod}"
major=${full%%.*}
rest=${full#*.}
floor="${major}.${rest%%.*}"

if [[ -n "${GO_DL_JSON:-}" ]]; then
  releases=$(cat "${GO_DL_JSON}") || die "cannot read ${GO_DL_JSON}"
else
  releases=$(curl -fsSL --retry 3 "${dl_url}") || die "cannot fetch supported Go releases from ${dl_url}"
fi

supported=$(echo "${releases}" |
  jq -r '.[] | select(.stable == true) | .version' 2>/dev/null |
  sed -nE 's/^go([0-9]+\.[0-9]+).*/\1/p' | sort -V -u) ||
  die "cannot parse the Go release list (expected the JSON from ${dl_url})"
[[ -n "${supported}" ]] || die "the Go release list contains no stable releases"

versions=("${floor}")
while read -r v; do
  if version_ge "${v}" "${floor}"; then versions+=("${v}"); fi
done <<<"${supported}"
matrix=$(printf '%s\n' "${versions[@]}" | sort -V -u | jq -R . | jq -sc .)
primary=$(echo "${matrix}" | jq -r '.[-1]')

if [[ "${format}" == "json" ]]; then
  jq -nS --arg p "${primary}" --argjson m "${matrix}" '{primary: $p, matrix: $m}'
else
  echo "primary=${primary}"
  echo "matrix=${matrix}"
fi
