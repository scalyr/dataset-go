# Patches for opentelemetry-collector-contrib

`scripts/test-otel-contrib.sh` applies every `*.patch` file here to its contrib checkout, in
name order, before it runs contrib's checks. The `OTel Contrib Tests` workflow and
`make test-otel-contrib` both run that script.

A patch is for a dataset-go change that intentionally changes behavior contrib's exporter tests
assert on. The matching contrib change can only land in the Renovate PR that bumps dataset-go to
the release containing that change. Until then, the patch lets this repo's contrib check stay
green and show that nothing else breaks.

- **Name:** `<JIRA-KEY>-<what-it-changes>.patch`.
- **Content:** make the change in a contrib checkout and save `git diff` from contrib's root.
  Put a short header above the diff (git ignores it) that says why the patch exists, links the
  contrib PR, and says when to delete it.
- **Lifetime:** delete the patch once the contrib PR is merged. Until you do, the script logs
  `skipped, contrib already contains it; delete the patch`.
- **Skipped, not fatal:** a patch that does not apply is skipped and logged. It does not fail
  the run, because contrib main keeps moving. A skipped patch that is still needed shows up as
  the contrib test it was meant to fix failing.
