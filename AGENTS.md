# AGENTS.md

Guidance for AI coding agents (Claude Code, Codex, Cursor, Copilot, ...) working in this repository.
`CLAUDE.md` imports this file. Keep the content here, not there.

## What this library is, and who depends on it

`github.com/scalyr/dataset-go` is a Go client for the DataSet
[`addEvents`](https://app.scalyr.com/help/api#addEvents) API. It exists to serve one consumer: the
OpenTelemetry Collector
[`datasetexporter`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/datasetexporter).
Renovate updates that exporter to each new release, so:

- **Exported identifiers in `pkg/` are a contract with opentelemetry-collector-contrib.** Renaming,
  removing or changing the signature of anything exported breaks their build. Prefer additive changes.
  If a breaking change is unavoidable, say so in the PR description.
- **The library runs inside a collector process.** Never call `Logger.Fatal`, `os.Exit` or `panic` on
  any path that input data can reach. Log the problem, count it in `statistics`, and drop or return an
  error. The existing `Logger.Fatal` in `processEvents` is a known bug (DPDV-20058), not a pattern to
  copy.
- Every release goes into the `User-Agent` (`pkg/version/version.go`), and DataSet usage is tracked by
  it.

## Architecture in one paragraph

Read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) before you change `pkg/client`, `pkg/buffer` or
`pkg/session_manager`. Its codes (`D1`, `U2`, `C3`, `I2.2`, ...) explain why the design is what it is.
In short, the collector calls `DataSetClient.AddEvents` in parallel. Events are grouped into sessions
by a key built from `group_by` fields and published through `session_manager`. One goroutine per
session (`processEvents`) packs them into a `buffer.Buffer`, which can hold at most `LimitBufferSize`
(about 5.9 MB before compression, see `D3`). Full or expired buffers are published to a second topic.
Its subscriber sends each one with `sendAddEventsBuffer`, one request at a time per session (`D1`),
with retries. Errors switch the whole client into a mode that
rejects new batches (see "Error Propagation").

## Performance is a requirement

This code is on the ingestion path for every log and span the exporter forwards. Throughput and
allocations per event matter more than brevity or elegance. The hot path has two halves. Per event:
`AddEvents` → `session_manager.Pub` → `processEvents` → `Buffer.AddBundle`. Per buffer:
`sendAddEventsBuffer` → `Buffer.Payload` → `ApiRequest.HttpRequest` (gzip) → HTTP. On both halves:

- Do not add work for each event: no `reflect`, no `fmt.Sprintf` for keys or logging, no repeated
  JSON marshalling of the same value, no `time.Now()` in tight loops when one call per batch is enough.
- Preallocate slices and maps when the size is known. Reuse buffers rather than reallocating them.
  Keep the existing byte-level serialization instead of building intermediate structures.
- Do not hold a mutex across I/O or channel sends. Do not create unbounded goroutines or channels. The
  design gives each session one goroutine (`I3`).
- Log levels: no `Info` or `Debug` logs for each event. Guard expensive `zap` fields with
  `Logger.Core().Enabled(...)` if you need them.
- **Stress test every hot-path change** with `make stress-test` (the same two scenarios as CI), on
  `main` and on your branch. Put the before and after numbers in the PR. Each run writes two files to
  `examples/stress/`:
  - `out-<events>-<buckets>.log` is the console log. Its final `Transfer Stats` line has
    `processingS`, `throughputMBpS` and `successRate`.
  - `log-<version>-<ms>.log` is a TSV written every second, with event and buffer counters
    (`EvDropped`, `EvBroken`, ...), `HeapAlloc` and `Mallocs`.

  Compare `processingS`, the dropped and broken counts (these must stay 0) and the peak `HeapAlloc`.
  A green Windows stress job in CI proves nothing yet. The script does not actually run there
  (DPDV-20231).
- There are no Go benchmarks yet. Adding them and a `bench-compare` workflow is DPDV-20230. Until
  then, if a PR claims a speed-up, it should include a `testing.B` benchmark run with `-benchmem`.

## Commands

Run `make help` for the full list. These are the targets that matter:

| Command                       | Use it to                                                                   |
| ----------------------------- | --------------------------------------------------------------------------- |
| `make test-unit`              | Run the fast feedback loop: all packages, `-race`, no `long_running` tests  |
| `make test`                   | Run all tests, including the `long_running` ones (as CI does)               |
| `make lint`                   | Run the pre-commit hooks: gofumpt, goimports, golangci-lint, staticcheck, errcheck |
| `make test-scripts`           | Run the tests for the shell scripts in `scripts/`                           |
| `make stress-test`            | Stress test the library through `examples/stress` (takes a few minutes)     |
| `make build-examples`         | Check that every module in `examples/` still builds against the library     |
| `make test-many-times COUNT=5`| Find flaky tests (the default `COUNT` is 50)                                |
| `make check`                  | Run lint, script tests, all tests and the example builds. **Run it before opening a PR.** |

`make pre-commit-install` installs the hooks and the Go tools they need (`scripts/install-dev-tools.sh`).

## Tests

- Always run tests with `-race`, as the Make targets do. The library is concurrent by design, and race
  failures here are real bugs.
- Tests that take more than a few seconds go in a `*_long_running_test.go` file with
  `//go:build long_running` (see `pkg/client/add_events_long_running_test.go`).
- Cover the happy path, sad path, edge cases, seams (HTTP via `httptest.Server`, JSON payload shape)
  and conformance (the `addEvents` request format, the 6 MB limit). The payload fixtures are in
  `test/testdata/`.
- Do not use `time.Sleep` to wait for asynchronous work if you can poll `Statistics()` or wait for a
  signal instead. Sleep-based tests are the main source of flaky tests here.
- Script changes need a test in `scripts/*_test.sh`. Those tests run offline with fixture trees and
  fake binaries on `PATH`. Copy the existing ones.

## Conventions

- New files start with the Apache 2.0 license header used throughout the repo.
- Formatting and linting are enforced by `.pre-commit-config.yaml` (gofumpt, goimports) and
  `.golangci.yml`. Run `make lint` and do not hand-format.
- Commits use Conventional Commits with the Jira key, e.g. `fix(client): [DPDV-20058] drop oversized
  bundles instead of exiting`. PRs follow `.github/pull_request_template.md`.
- `examples/*` are separate Go modules that `replace` the root module with `../..`. If you change a
  dependency, run `go mod tidy` in each of them (`make build-examples` does this).

## Releases and Go versions (do not do by hand)

- Never push tags. Never edit `pkg/version/version.go`, except to request a minor or major release on
  purpose. The `release-tag` workflow creates a patch release for every merge to `main` that changes
  what consumers get. See README → "Releases" and `scripts/next-release.sh`.
- Go versions are not hard-coded anywhere. CI derives them from `go.mod` with
  `scripts/go-versions.sh`. See README → "Go versions in CI".
- Dependabot PRs are merged automatically once the required checks pass. A change that makes CI
  flaky blocks every dependency update.
