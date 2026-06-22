# CLAUDE.md

Guidance for Claude Code when working inside `pkg/stream/preflight`. The planned check set lives in `docs/migration_preflight_issue.md`; consult it before designing a new check.

## Package shape

- `preflight.go` — `Check` interface (`Name()` + `Run(ctx) ([]Finding, error)`), `Finding`, `CheckResult`, `Report`, `Run(ctx, []Check, ...RunOption)` engine.
- `printer.go` — `ReportPrinter{Report}` is the only thing that formats reports. The `Report` struct itself stays pure data.
- `builder.go` — `Builder` struct, `Builders` registry slice, per-category builder functions (`BuildConnectivityChecks`, …), `BuildChecks(cfg, selected)`.
- One file per concrete check (`connectivity.go`, future `wal_level.go`, …).

## Adding a new check

Adding a check is meant to be a small, mechanical edit. Keep it that way.

1. **Pick a category.** Categories group checks of the same concern (`connectivity`, `replication`, `access`, `schema`, `resources`).
   - Joining an existing category: skip to step 2.
   - Creating a new one: add a `Category` constant in `preflight.go`, a builder func + `Builders` entry in `builder.go`, and a boolean flag on `checkCmd` in `cmd/root_cmd.go`. The flag string must match `Builder.Flag`.
2. **Implement the check.** New struct in `<thing>.go`, satisfying the `Check` interface.
   - **Every `Finding` is blocking.** A check that finds nothing wrong returns a `nil` slice.
   - **Return `error` only when the check itself couldn't run** (timeout, internal bug, malformed input). A detected problem is a `Finding`, not an error.
   - **Put remediation in `Finding.Message`** — the user should be able to act on it without reading source.
3. **Materialise instances in the category builder** (e.g. `BuildConnectivityChecks`). The builder is the applicability gate: it reads `*stream.Config` and decides which instances are relevant. Inapplicable checks are silently omitted today; an explicit "skipped: <reason>" mechanism is deferred (see `docs/migration_preflight_issue.md` "Architecture decisions" #6).
4. **Tests.** Unit-test the check directly against mocked dependencies (`internal/postgres/mocks` has the postgres conn mock). For new categories, exercise the builder selection path through the cmd layer too.

## Do not

- Do not add `init()`-time registration, dependency injection frameworks, or other indirection — `Builders` is the registry, keep it a plain literal slice.
- Do not move rendering logic onto `Report`. `ReportPrinter` owns formatting; `Report` stays data-only.
- Do not import `pkg/stream` from anywhere except `builder.go`. Engine code (`preflight.go`, `printer.go`, individual check files) stays stream-agnostic so it can be reused.
