# Copyright 2023 SentinelOne, Inc.
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

# build options are from https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/Makefile.Common
# to make out library compatible with open telemetry

SHELL = /bin/bash
ifeq ($(shell uname -s),Windows)
	.SHELLFLAGS = /e /o pipefail /c
else
	.SHELLFLAGS = -e -o pipefail -c
endif

GO_BUILD_TAGS=""
GOTEST_OPT?= -v -race -timeout 300s -parallel 4 -count=1 --tags=$(GO_BUILD_TAGS)
GOTEST_LONG_RUNNING_OPT?= -v -race -timeout 600s -parallel 4 -count=1 -tags=long_running,$(GO_BUILD_TAGS)
GOTEST_OPT_WITH_COVERAGE = $(GOTEST_OPT) -coverprofile=coverage.txt -covermode=atomic
GOTEST_OPT_WITH_COVERAGE_LONG_RUNNING=$(GOTEST_LONG_RUNNING_OPT) -coverprofile=coverage.txt -covermode=atomic
GOCMD?= go
GOTEST=$(GOCMD) test

.DEFAULT_GOAL := pre-commit-run

# Targets with a `## description` are listed by `make help`.
.PHONY: help
help: ## List the documented targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  %-22s %s\n", $$1, $$2}'

.PHONY: check
check: ## Everything to run before opening a PR
	@# Sequential even under -j: pre-commit may rewrite files the other steps read.
	$(MAKE) lint
	$(MAKE) govulncheck
	$(MAKE) test-scripts
	$(MAKE) test
	$(MAKE) build-examples
	$(MAKE) test-otel-contrib

.PHONY: pre-commit-install
pre-commit-install: ## Install the pinned dev tools (needs uv and go) and the git hook
	./scripts/install-dev-tools.sh

.PHONY: pre-commit-run
pre-commit-run: ## Run all pre-commit hooks on all files
	uv run --frozen pre-commit run -a

.PHONY: dev-tools-update
dev-tools-update: ## Upgrade the pinned Python and Go dev tools (Dependabot does this weekly)
	uv lock --upgrade
	cd tools && go get -u tool && go mod tidy

.PHONY: lint
lint: pre-commit-run ## Lint and format checks (alias of pre-commit-run)

.PHONY: go-versions
go-versions: ## Print the Go versions CI tests with
	./scripts/go-versions.sh --json

.PHONY: test-scripts
test-scripts: ## Test the shell scripts in scripts/
	./scripts/go-versions_test.sh
	./scripts/next-release_test.sh
	./scripts/bump-version_test.sh
	./scripts/run-stress-test_test.sh
	./scripts/test-otel-contrib_test.sh
	./scripts/install-dev-tools_test.sh
	./scripts/go-tool_test.sh

.PHONY: build
build:
	echo "Done"

.PHONY: test
test: test-all ## Alias of test-all

.PHONY: test-unit
test-unit: ## Unit tests with -race, without long_running tests
	time $(GOTEST) $(GOTEST_OPT) ./...

.PHONY: test-all
test-all: ## All tests with -race, including long_running ones
	time $(GOTEST) $(GOTEST_LONG_RUNNING_OPT) ./...

.PHONY: test-many-times
test-many-times: ## Run `make test` COUNT times (default 50) to find flaky tests
	set -e; \
	if [ "x$(COUNT)" == "x" ]; then \
		COUNT=50; \
	else \
		COUNT=$(COUNT); \
	fi; \
	prefix=out-test-many-times-; \
	rm -rfv $${prefix}*; \
	for i in `seq 1 $${COUNT}`; do \
		echo "Running test $${i} / $${COUNT} - BEGIN"; \
		make test 2>&1 | tee $${prefix}-$${i}.log | awk '{print "'$${i}'/'$${COUNT}'", $$0; }' || exit 0; \
		echo "Running test $${i} / $${COUNT} - END"; \
	done; \
	echo "Grep for FAIL - no lines should be found"; \
	! grep -H FAIL $${prefix}-*.log;

.PHONY: coverage
coverage: coverage-all ## Alias of coverage-all

.PHONY: coverage-unit
coverage-unit: ## Coverage of unit tests into coverage.html
	$(GOTEST) $(GOTEST_OPT_WITH_COVERAGE) ./...
	$(GOCMD) tool cover -html=coverage.txt -o coverage.html

.PHONY: coverage-all
coverage-all: ## Coverage of all tests into coverage.html
	$(GOTEST) $(GOTEST_OPT_WITH_COVERAGE_LONG_RUNNING) ./...
	$(GOCMD) tool cover -html=coverage.txt -o coverage.html

.PHONY: build-examples
build-examples: ## Build every module in examples/ (output discarded)
	for d in examples/*; do \
		echo "Build example: $${d}"; \
		(cd $${d}; go mod tidy && go build -race -o /dev/null) || exit 1; \
	done;

# The two scenarios CI runs, see .github/workflows/stress-tests.yaml.
STRESS_EVENTS ?= 10000
.PHONY: stress-test
stress-test: ## Stress test: every bucket used once, then max congestion
	./scripts/run-stress-test.sh $(STRESS_EVENTS) $(STRESS_EVENTS)
	./scripts/run-stress-test.sh $(STRESS_EVENTS) -1

# Clones opentelemetry-collector-contrib once (CONTRIB_DIR) and resets it to the
# latest main on every run, see the script's header.
.PHONY: test-otel-contrib
test-otel-contrib: ## Run contrib's datasetexporter lint and tests against this checkout (network)
	./scripts/test-otel-contrib.sh

# Pinned, so a govulncheck release cannot change the result of an unchanged commit.
GOVULNCHECK_VERSION ?= v1.10.0
.PHONY: govulncheck
govulncheck: ## Check this module's code and dependencies for known vulnerabilities (network)
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

.PHONY: test-ssl-certificates
test-ssl-certificates: ## Check the readme example accepts good and rejects bad TLS certs (network)
	cd scripts; ./test-ssl-certificates.sh

.PHONY: test-ci
test-ci: test-all build-examples test-ssl-certificates ## All tests, example builds and TLS checks
