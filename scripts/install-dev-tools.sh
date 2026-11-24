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
# Installs the pinned dev tools and the pre-commit git hook. Used by
# `make pre-commit-install` and by the Code Quality workflow.
#
# - Python tools (pre-commit and the tools its hooks run) come from uv.lock.
# - Go tools come from tools/go.mod; building them here only warms the cache
#   for scripts/go-tool.sh; nothing is put on PATH.
#
# Usage: scripts/install-dev-tools.sh

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"

if ! command -v uv >/dev/null; then
  echo "uv is required: https://docs.astral.sh/uv/getting-started/installation/" >&2
  exit 1
fi
if ! command -v go >/dev/null; then
  echo "go is required: https://go.dev/doc/install" >&2
  exit 1
fi

uv sync --frozen
# Writes .git/hooks/pre-commit pointing at the .venv interpreter, so `git
# commit` also runs the locked pre-commit.
uv run --frozen pre-commit install
go -C "${root}/tools" build tool
