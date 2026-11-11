#!/usr/bin/env bash
#
# Writes a release version and date into pkg/version/version.go, whose values
# end up in the client User-Agent. Run by the release-tag workflow before it
# commits and tags, so the constant always matches the tag it ships under.
#
# Fails, leaving the file untouched, unless each of the `Version = "..."` and
# `ReleasedDate = "..."` lines occurs exactly once: a refactor of version.go
# must break the release loudly rather than ship a stale constant.
#
# Usage:
#   scripts/bump-version.sh <X.Y.Z> <YYYY-MM-DD>
#
# Environment (mainly for tests):
#   VERSION_FILE  file to rewrite (default: pkg/version/version.go)

set -euo pipefail

die() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ $# -eq 2 ]] || die "usage: $0 <X.Y.Z> <YYYY-MM-DD>"
version=$1
date=$2
[[ "${version}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
  die "version '${version}' is not X.Y.Z (no 'v' prefix, no suffix)"
[[ "${date}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "date '${date}' is not YYYY-MM-DD"

file=${VERSION_FILE:-$(cd "$(dirname "$0")/.." && pwd)/pkg/version/version.go}
[[ -f "${file}" ]] || die "${file} not found"

for name in Version ReleasedDate; do
  count=$(grep -cE "^[[:space:]]*${name}[[:space:]]*=[[:space:]]*\"[^\"]*\"[[:space:]]*$" "${file}" || true)
  [[ "${count}" == 1 ]] || die "expected exactly one '${name} = \"...\"' line in ${file}, found ${count}"
done

tmp=$(mktemp -d)
trap 'rm -rf "${tmp}"' EXIT
sed -E \
  -e "s/^([[:space:]]*Version[[:space:]]*=[[:space:]]*)\"[^\"]*\"/\1\"${version}\"/" \
  -e "s/^([[:space:]]*ReleasedDate[[:space:]]*=[[:space:]]*)\"[^\"]*\"/\1\"${date}\"/" \
  "${file}" >"${tmp}/version.go"

unformatted=$(gofmt -l "${tmp}/version.go") || die "gofmt failed on the rewritten file"
[[ -z "${unformatted}" ]] || die "rewritten ${file} is not gofmt-clean"

# cat, not mv: keeps the file's mode and any hard links.
cat "${tmp}/version.go" >"${file}"
