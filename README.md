# DataSet-Go

[![Go Reference](https://pkg.go.dev/badge/github.com/scalyr/dataset-go.svg)](https://pkg.go.dev/github.com/scalyr/dataset-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/scalyr/dataset-go)](https://goreportcard.com/report/github.com/scalyr/dataset-go)
[![Check code quality](https://github.com/scalyr/dataset-go/actions/workflows/code-quality.yaml/badge.svg)](https://github.com/scalyr/dataset-go/actions/workflows/code-quality.yaml)
[![Check code quality](https://github.com/scalyr/dataset-go/actions/workflows/unit-tests.yaml/badge.svg)](https://github.com/scalyr/dataset-go/actions/workflows/unit-tests.yaml)
[![TruffleHog Secrets Scan](https://github.com/scalyr/dataset-go/actions/workflows/secrets-scanner.yaml/badge.svg)](https://github.com/scalyr/dataset-go/actions/workflows/secrets-scanner.yaml)
[![codecov](https://codecov.io/gh/scalyr/dataset-go/branch/main/graph/badge.svg?token=IFTJDLGEF5)](https://codecov.io/gh/scalyr/dataset-go)
[![Version](https://img.shields.io/github/tag/scalyr/dataset-go.svg)](https://github.com/scalyr/dataset-go/releases)

This repository holds the source code for the Go wrapper around [DataSet API](https://app.scalyr.com/help/api).

To learn more about DataSet, visit https://www.dataset.com.

## Features

The DataSet API supports following API calls:

* [addEvents](https://app.scalyr.com/help/api#addEvents) - This endpoint inserts one or more structured or unstructured log events. It can scale horizontally to ingest 100s of TBs per day.


## Examples

```go
// create logger
logger := zap.Must(zap.NewDevelopment())
// read configuration from env variables
cfg, err := config.New(config.FromEnv())
if err != nil { panic(err) }

// build client
cl, err := client.NewClient(cfg, &http.Client{}, logger, nil, nil)
if err != nil { panic(err) }

// send bundles
err = cl.AddEvents(makeBundles())
if err != nil { panic(err) }

// Finish event processing
err = cl.Shutdown()
if err != nil { panic(err) }
```

Full example can be found in [examples/readme/](examples/readme/main.go)

### All examples:

Examples are located in the [examples](examples) folder.
* [simple client](examples/client/main.go) - simple client
* [readme](examples/readme/main.go) - full example from readme


## Release Notes and Changelog

For release notes please see [RELEASE_NOTES.md](RELEASE_NOTES.md) document and for changelog,
see [CHANGELOG.md](CHANGELOG.md) document.

## Developing

### Install Dev Tools

Run following script - [install-dev-tools.sh](scripts/install-dev-tools.sh) - to install
tools needed by `pre-commit`.

### Pre-Commit Hooks

[Pre-commit](https://pre-commit.com) is used to automatically run checks including Black formatting
prior to a git commit.

To use pre-commit:

- Use one of the [Installation Methods](https://pre-commit.com/#install) from the documentation.
- Install the hooks with `pre-commit install`.
- To manually execute the pre-commit hooks (including black), run `pre-commit run --all-files` (
  run it on all the files) or ``pre-commit run`` (run it only on staged files).

#### Pre-commit Configuration

- `.pre-commit-config.yaml` configures the scripts run by pre-commit

To update the Pre-commit hooks , run `pre-commit autoupdate`. This will update
`.pre-commit-config.yaml` and will need to be committed to the repository.

### Go versions in CI

The GitHub workflows do not hard-code any Go version. Each workflow's
`versions` job runs [`go-versions.sh`](scripts/go-versions.sh), which computes
them at run time:

- `matrix` &mdash; the `go` directive of the root [`go.mod`](go.mod) (the floor),
  plus every currently supported Go release (from <https://go.dev/dl/?mode=json>)
  at or above it. Releases below the floor are never included: with
  `GOTOOLCHAIN=local` they cannot build the module.
- `primary` &mdash; the highest `matrix` entry, used by single-version jobs
  (e.g. code quality).

There is nothing to regenerate. When Dependabot raises the `go` directive, or a
new Go release ships, the next CI run uses the new versions. The nightly runs
make sure that happens within a day. A new Go release can therefore turn a
nightly run red without any code change. `make go-versions` prints the current
values, and `make test-scripts` runs the script's tests.

### Automated updates

The repository is meant to keep itself current without a maintainer.
[Dependabot](.github/dependabot.yml) opens one grouped PR per module, and one
for GitHub Actions, every week. The
[`auto-merge`](.github/workflows/auto-merge.yaml) workflow approves each one and
enables squash auto-merge, so GitHub merges it once the required checks pass.

It only uses `GITHUB_TOKEN`, so the merge commit does not trigger the `push`
workflows on `main`. The nightly scheduled runs cover `main` instead.

Required repository settings:

1. *Settings &rarr; Actions &rarr; General*: "Allow GitHub Actions to create
   and approve pull requests" must stay enabled, because `main` requires one
   approving review.
2. In the branch protection for `main`, require these status checks:
   `Unit Tests result`, `Stress Tests result` and `Code Quality result`. The
   matrix job names contain Go versions and change over time, so require the
   `... result` jobs instead. **Without required checks, auto-merge merges a
   Dependabot PR as soon as it is approved.**

### Releases

The [`release-tag`](.github/workflows/release-tag.yaml) workflow creates the
next **patch** release (`vX.Y.Z` &rarr; `vX.Y.Z+1`) for a commit on `main` that
passed Unit Tests. It does so only if something consumers get changed since
the latest tag:

- a non-test `.go` file of the module (outside the nested `examples/*`
  modules, `test/`, `testdata/` and `vendor/`), or
- the `go`, `toolchain` or `retract` directives in `go.mod`, or
- a `go.mod` requirement on a module that the library's non-test packages link
  on linux, darwin or windows.

Tests, examples, scripts, workflows, docs and test-only dependencies such as
testify never release. [`scripts/next-release.sh`](scripts/next-release.sh)
holds the rules, and `scripts/next-release.sh HEAD` shows what would happen.

A release commits the new version to `pkg/version/version.go` (used in the
client User-Agent) as `chore(release): vX.Y.Z` and tags that commit. Dependabot
merges fire no `push` workflows (see above), so they are released after the
nightly Unit Tests run.

For a minor or major release, set `Version` in `pkg/version/version.go` to the
new version (e.g. `"0.22.0"`) in a PR. Once merged and tested, the workflow
releases `v0.22.0` instead of the next patch. Do not push release tags by hand.
A tag that is not on `main` stops the workflow with an error until it is
removed.

Required repository settings:

1. Secret `RELEASE_TOKEN`: a fine-grained personal access token with
   *Contents: read and write* on this repository. `GITHUB_TOKEN` cannot be used:
   it cannot push past the required review on `main`.
2. The token's owner must be allowed to bypass the branch protection or
   ruleset on `main`, and any tag ruleset covering `v*`. Otherwise the push is
   rejected and the workflow fails.

When the token expires or is rotated, update the secret.

## Contributing

In the future, we will be pushing guidelines on how to contribute to this repository.  For now, please just
feel free to submit pull requests to the `main` branch and we will work with you.

## Copyright, License, and Contributors Agreement

Copyright 2023 SentinelOne, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in
compliance with the License. You may obtain a copy of the License in the [LICENSE](LICENSE) file, or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

By contributing you agree that these contributions are your own (or approved by your employer) and you
grant a full, complete, irrevocable copyright license to all users and developers of the project,
present and future, pursuant to the license of the project.

## Architecture

see detailed [Architecture description](docs/ARCHITECTURE.md)
