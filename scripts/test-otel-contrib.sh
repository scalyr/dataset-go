#!/usr/bin/env bash
#
# Runs the checks of the OpenTelemetry Collector `datasetexporter` against this
# checkout of dataset-go instead of the release contrib pins, so a change that
# would break the exporter fails here rather than in the next Renovate bump.
# The checks are contrib's own per-component make targets (Makefile.Common):
# the ones its CI (build-and-test.yml) runs on every module, as CONTRIBUTING.md
# asks of a contrib PR.
#
# Usage: scripts/test-otel-contrib.sh
#
# Environment:
#   CONTRIB_DIR   - where contrib is checked out and reused between runs
#                   (default ${XDG_CACHE_HOME:-$HOME/.cache}/dataset-go/opentelemetry-collector-contrib).
#                   EVERY RUN DISCARDS ALL LOCAL CHANGES IN IT. Do not point it at your own clone;
#                   the script refuses a directory whose origin is not CONTRIB_REPO.
#   CONTRIB_REPO  - contrib clone URL (default the upstream GitHub repo)
#   CONTRIB_REF   - branch to test against (default main)
#   CONTRIB_TARGETS - contrib make targets run, in order, in the exporter directory after
#                   `make tidy` (default: see DEFAULT_TARGETS). Contrib's make variables,
#                   e.g. GOTEST_OPT, can be overridden through the environment.
#   GOTOOLCHAIN   - default auto: contrib's go.mod may need a newer Go than is installed
#
# Makefile.Common targets left out of the default, and why:
#   - test-with-cover, test-with-junit*, do-*-with-cover, buildtest, runbuilttest:
#     the same tests as `test` and `mod-integration-test`, plus reporting.
#   - test-twice, run-changed-tests, for-affected-components: contrib's flake
#     hunting for changed .go files; a dataset-go bump changes only go.mod, so
#     contrib does not run them on it either.
#   - mod-integration-sudo-test: needs root; the exporter has no Sudo tests.
#   - check-stability: checks the exporter's coverage profile, which is contrib's.
#   - generate, mdatagen, schemagen: outputs depend on metadata.yaml and the
#     exporter's exported Config, which uses no dataset-go types.
#   - apidiff-*: the exporter's own API (no dataset-go types), against a baseline
#     built from the base branch.
#   - fmt, gci, modernize, addlicense, misspell-correction, toolchain: rewrite
#     contrib's sources; `lint` already runs the read-only checks.
#   - checklinks (Docker, markdown links), moddownload (part of tidy), help, pwd,
#     common (= lint test).

set -euo pipefail

usage() {
  sed -n 's/^# \(Usage: .*\)/\1/p' "$0" >&2
  exit 2
}
[[ $# -eq 0 ]] || usage

CONTRIB_DIR="${CONTRIB_DIR:-${XDG_CACHE_HOME:-${HOME}/.cache}/dataset-go/opentelemetry-collector-contrib}"
CONTRIB_REPO="${CONTRIB_REPO:-https://github.com/open-telemetry/opentelemetry-collector-contrib.git}"
CONTRIB_REF="${CONTRIB_REF:-main}"
# test-no-race and benchmark find nothing in the exporter today; they are cheap
# and pick up `//go:build !race` tests or benchmarks contrib adds later.
DEFAULT_TARGETS="lint govulncheck test test-no-race mod-integration-test benchmark"
CONTRIB_TARGETS="${CONTRIB_TARGETS-${DEFAULT_TARGETS}}"
export GOTOOLCHAIN="${GOTOOLCHAIN:-auto}"
EXPORTER=exporter/datasetexporter

die() {
  echo "test-otel-contrib: $*" >&2
  exit 1
}

read -r -a targets <<<"${CONTRIB_TARGETS}"
[[ ${#targets[@]} -gt 0 ]] || die "CONTRIB_TARGETS is empty, nothing would be checked"
# Run from our Makefile, these would hand our command-line variables and
# jobserver to contrib's make.
unset MAKEFLAGS MFLAGS MAKELEVEL

root="$(cd "$(dirname "$0")/.." && pwd -P)"
module="$(awk '$1 == "module" { print $2; exit }' "${root}/go.mod")"
[[ -n "${module}" ]] || die "no module line in ${root}/go.mod"

# Either clone, or reset the cached clone to the latest CONTRIB_REF. An empty
# directory counts as missing, so a pre-created CI path works.
if [[ ! -e "${CONTRIB_DIR}" ]] || [[ -d "${CONTRIB_DIR}" && -z "$(ls -A "${CONTRIB_DIR}")" ]]; then
  echo "Cloning ${CONTRIB_REPO} (${CONTRIB_REF}) into ${CONTRIB_DIR}"
  mkdir -p "$(dirname "${CONTRIB_DIR}")"
  git clone --quiet --depth 1 --branch "${CONTRIB_REF}" "${CONTRIB_REPO}" "${CONTRIB_DIR}"
else
  # The reset below is destructive, so check this is our clone: the top of its
  # own work tree (not a subdirectory of some other repo) with the expected origin.
  [[ -d "${CONTRIB_DIR}" ]] || die "${CONTRIB_DIR} exists and is not a directory"
  dir="$(cd "${CONTRIB_DIR}" && pwd -P)"
  top="$(git -C "${dir}" rev-parse --show-toplevel 2>/dev/null)" || top=""
  [[ -n "${top}" && "$(cd "${top}" && pwd -P)" == "${dir}" ]] ||
    die "${CONTRIB_DIR} is not a git clone; refusing to touch it (remove it or set CONTRIB_DIR)"
  # The stored URL, not `remote get-url`: that applies url.*.insteadOf, so a
  # user rewriting https://github.com/ to ssh would never match CONTRIB_REPO.
  origin="$(git -C "${dir}" config --get remote.origin.url)" || origin=""
  [[ "${origin}" == "${CONTRIB_REPO}" ]] ||
    die "${CONTRIB_DIR} has origin '${origin}', expected '${CONTRIB_REPO}'; refusing to reset it"
  echo "Updating ${CONTRIB_DIR} to the latest ${CONTRIB_REF}"
  git -C "${dir}" fetch --quiet --depth 1 origin "${CONTRIB_REF}"
  git -C "${dir}" checkout --quiet --force --detach FETCH_HEAD
  git -C "${dir}" clean --quiet -ffdx
fi

echo "Testing ${module} from ${root}"
echo "against ${CONTRIB_REPO} ${CONTRIB_REF} at $(git -C "${CONTRIB_DIR}" rev-parse HEAD)"

[[ -f "${CONTRIB_DIR}/${EXPORTER}/go.mod" ]] || die "${EXPORTER}/go.mod not found in ${CONTRIB_DIR}"
cd "${CONTRIB_DIR}/${EXPORTER}"

# A structured go.mod edit, then contrib's `make tidy` regenerates go.sum: the
# same change a version bump in contrib would make, with this checkout
# standing in for the release.
go mod edit "-replace=${module}=${root}"
for target in tidy "${targets[@]}"; do
  echo "make ${target}"
  make "${target}"
done
