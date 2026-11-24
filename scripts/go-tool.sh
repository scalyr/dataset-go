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
# Runs a Go dev tool pinned in tools/go.mod, in the current directory.
#
# Not `go tool -modfile=tools/go.mod`: Go then still treats the repo root as
# the main module directory, and once CI has run `go mod vendor` it checks
# tools/go.mod against the root vendor/modules.txt and fails ("inconsistent
# vendoring"); `go tool` ignores -mod and GOFLAGS. Resolving the tool inside
# tools/ (no vendor/ there) and running the cached binary here avoids that.
#
# Usage: scripts/go-tool.sh TOOL [ARGS...]

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 TOOL [ARGS...]" >&2
  exit 2
fi

tools_dir="$(cd "$(dirname "$0")/../tools" && pwd)"
# `go tool -n` builds the tool into the build cache and prints its path. A
# caller's GOFLAGS (e.g. -mod=vendor) or go.work must not change what that is.
bin=$(GOFLAGS='' GOWORK=off go -C "${tools_dir}" tool -n "$1")
shift
exec "${bin}" "$@"
