# DPDV-20148 — AI tooling (quick spec + plan)

## Acceptance criteria
- AC1: `AGENTS.md` exists and covers: consumer (OTel datasetexporter), hot path, performance rules,
  commands, testing, conventions, releases. `CLAUDE.md` imports it with `@AGENTS.md`.
- AC2: When `scripts/run-stress-test.sh` is run without arguments, it shall use 10000 events and 10000 buckets.
- AC3: When the stress binary exits non-zero, `run-stress-test.sh` shall exit non-zero.
- AC4: If an argument is not an integer (`-1` allowed for buckets), the script shall exit non-zero with usage.
- AC5: The stress log shall be written to `examples/stress/out-<E>-<B>.log` (CI artifact glob).
- AC6: `make help` lists the documented targets. `make lint`, `make stress-test` and `make check` exist. `foo` is gone.
- AC7: After `make build-examples`, `git status` shows no new untracked files.
- AC8: CI's code-quality job runs `scripts/run-stress-test_test.sh`.

## Tasks
1. Test-first: `scripts/run-stress-test_test.sh` (AC2–AC5), then fix the script.
2. Makefile + `examples/stress/.gitignore` + CI line (AC6–AC8).
3. `AGENTS.md`, `CLAUDE.md`, README pointer (AC1).
