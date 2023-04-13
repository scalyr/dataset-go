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
GO_BUILD_TAGS=""
GOTEST_OPT?= -race -timeout 300s -parallel 4 -count=1 --tags=$(GO_BUILD_TAGS)
GOTEST_INTEGRATION_OPT?= -race -timeout 360s -parallel 4
GOTEST_OPT_WITH_COVERAGE = $(GOTEST_OPT) -coverprofile=coverage.txt -covermode=atomic
GOTEST_OPT_WITH_INTEGRATION=$(GOTEST_INTEGRATION_OPT) -tags=integration,$(GO_BUILD_TAGS) -run=Integration -coverprofile=integration-coverage.txt -covermode=atomic
GOCMD?= go
GOTEST=$(GOCMD) test

.DEFAULT_GOAL := pre-commit-run

.PHONY: pre-commit-install
pre-commit-install:
	pre-commit install
	./scripts/install-dev-tools.sh

.PHONY: pre-commit-run
pre-commit-run:
	pre-commit run -a

build:
	echo "Done"

.PHONY: test
test:
	$(GOTEST) $(GOTEST_OPT) ./...

.PHONY: coverage
coverage:
	$(GOTEST) $(GOTEST_OPT_WITH_COVERAGE) ./...
	$(GOCMD) tool cover -html=coverage.txt -o coverage.html
