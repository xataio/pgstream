# CLAUDE.md

Guidance for Claude Code when working inside `pkg/stream/preflight`. The planned check set lives in `docs/migration_preflight_issue.md`; consult it before designing a new check.

## Package shape

- `preflight.go` — `Check` interface (`Name()` + `Run(ctx) ([]Finding, error)`), the optional `Detailer` / `Summarizer` interfaces described under [Reporting what a check observed](#reporting-what-a-check-observed), `Finding`, `CheckResult`, `Report`, `Run(ctx, []Check, ...RunOption)` engine. The engine calls each optional interface after `Run`, so a check populates them from state it gathered while running.
- `printer.go` — `ReportPrinter{Report}` is the only thing that formats reports, rendering each result's `Summary` beside its name. The `Report` struct itself stays pure data.
- `builder.go` — `Builder` struct (returns `[]Check` + optional cleanup), `Builders` registry slice, per-category builder functions (`BuildConnectivityChecks`, …), `BuildChecks(cfg, selected)`.
- One file per category of concrete checks (`connectivity.go`, `replication.go`, …).

The shared-conn primitive lives one floor down at `internal/postgres.LazyConn` so other callers can reuse it.

## Adding a new check

Adding a check is meant to be a small, mechanical edit. Keep it that way.

1. **Pick a category.** Categories group checks of the same concern (`connectivity`, `replication`, `access`, `schema`, `resources`).
   - Joining an existing category: skip to step 2.
   - Creating a new one: add a `Category` constant in `preflight.go`, a builder func + `Builders` entry in `builder.go`, and a boolean flag on `checkCmd` in `cmd/root_cmd.go`. The flag string must match `Builder.Flag`.
2. **Implement the check.** New struct in `<thing>.go`, satisfying the `Check` interface.
   - **Every `Finding` is blocking.** A check that finds nothing wrong returns a `nil` slice.
   - **Return `error` only when the check itself couldn't run** (timeout, internal bug, malformed input). A detected problem is a `Finding`, not an error.
   - **Put remediation in `Finding.Message`** — the user should be able to act on it without reading source.
3. **Report what it observed**, if it observed anything worth reporting — see [Reporting what a check observed](#reporting-what-a-check-observed). Most checks need none of this: a check that only passes or fails implements no optional interface.
4. **Materialise instances in the category builder** (e.g. `BuildConnectivityChecks`). The builder is the applicability gate: it reads `*stream.Config` and decides which instances are relevant. Inapplicable checks are silently omitted today; an explicit "skipped: <reason>" mechanism is deferred (see `docs/migration_preflight_issue.md` "Architecture decisions" #6).
   - **If checks in the category share a Postgres connection**, call `postgres.NewLazyConn(url)` in the builder, hand `src.Acquire` (a `postgres.AcquireFunc`) to every check, and return `src.Close` as the cleanup. See `BuildReplicationChecks` for the pattern. The engine runs sequentially, so the first check to call `Source(ctx)` opens the conn and the rest reuse it. A failed dial is memoised too — only one connection attempt happens, even if every check reports its own check error.
5. **Tests.** Unit-test the check directly against mocked dependencies (`internal/postgres/mocks` has the postgres conn mock). For new categories, exercise the builder selection path through the cmd layer too.

## Reporting what a check observed

Two optional interfaces, split by **data vs presentation**. Both read the state the check gathered during `Run`, and each feeds one report.

| | JSON | human |
| --- | --- | --- |
| `Details() map[string]any` | nested under the result's `details` key | — |
| `Summary() string` | — | rendered beside the check name |

- **Typed facts go in `Details`.** Keep values machine-readable: a size belongs here as a byte count, a version as a string. One fact, one representation — never a pre-rendered string beside the value it renders.
- **A headline worth one line goes in `Summary`.** Which facts lead and how they read is judgement specific to the check — a generic renderer over `Details` cannot infer it, and would print keys in alphabetical order. Formatting for the reader happens here, never in `Details`; `prettySize` renders byte counts.

Findings are unaffected: remediation belongs in `Finding.Message` whether or not a check reports anything else.

## Do not

- Do not add `init()`-time registration, dependency injection frameworks, or other indirection — `Builders` is the registry, keep it a plain literal slice.
- Do not move rendering logic onto `Report`. `ReportPrinter` owns formatting; `Report` stays data-only.
- Do not import `pkg/stream` from anywhere except `builder.go`. Engine code (`preflight.go`, `printer.go`, individual check files) stays stream-agnostic so it can be reused.
- Do not put display strings in `Details`, and do not let a check's SQL format them (`pg_size_pretty` and friends). `Details` carries typed values and `Summary` renders them, so the JSON and human reports cannot drift apart.
