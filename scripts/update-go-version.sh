#!/usr/bin/env bash
#
# Single source of truth for the Go version is the `go` directive in the root
# go.mod. This script derives the CI matrix from it and (re)generates
# .github/go-versions.json, which all GitHub workflows consume.
#
#   primary = <major>.<minor>            (used by single-version jobs)
#   matrix  = last 3 minors              (used by matrix jobs)
#
# Usage:
#   scripts/update-go-version.sh           regenerate .github/go-versions.json from go.mod
#   scripts/update-go-version.sh --check   fail if the committed file is out of sync (CI guard)

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
gomod="${repo_root}/go.mod"
config="${repo_root}/.github/go-versions.json"

# Extract e.g. "1.25" from a "go 1.25.0" directive.
full=$(grep -E '^go [0-9]' "${gomod}" | head -n1 | awk '{print $2}')
major=${full%%.*}
rest=${full#*.}
minor=${rest%%.*}

if [[ -z "${major}" || -z "${minor}" ]]; then
  echo "ERROR: could not parse go version from ${gomod} (found '${full}')" >&2
  exit 1
fi

primary="${major}.${minor}"
matrix=$(printf '["%s.%s","%s.%s","%s.%s"]' \
  "${major}" "$((minor - 2))" \
  "${major}" "$((minor - 1))" \
  "${major}" "${minor}")

# Keys are emitted sorted (-S) to match the repo's pretty-format-json pre-commit hook.
generated=$(jq -nS --arg p "${primary}" --argjson m "${matrix}" '{primary: $p, matrix: $m}')

if [[ "${1:-}" == "--check" ]]; then
  if ! diff -u <(jq -S . "${config}" 2>/dev/null || echo "MISSING") <(echo "${generated}" | jq -S .) >/dev/null; then
    echo "ERROR: ${config} is out of sync with go.mod (go ${primary})." >&2
    echo "       Run: scripts/update-go-version.sh" >&2
    exit 1
  fi
  echo "${config} is in sync with go.mod (go ${primary})."
  exit 0
fi

echo "${generated}" > "${config}"
echo "Updated ${config} from go.mod (go ${primary}):"
cat "${config}"
