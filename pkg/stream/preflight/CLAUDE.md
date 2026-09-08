# CLAUDE.md

Guidance for Claude Code when working inside `pkg/stream/preflight`. The planned check set lives in `docs/migration_preflight_issue.md`; consult it before designing a new check.

## Package shape

- `preflight.go` — `Check` interface (`Name()` + `Run(ctx) ([]Finding, error)`), the optional `Detailer` / `Summarizer` interfaces described under [Reporting what a check observed](#reporting-what-a-check-observed), `Finding`, `CheckResult`, `Report`, `Run(ctx, []Check, ...RunOption)` engine. The engine calls each optional interface after `Run`, so a check populates them from state it gathered while running.
- `printer.go` — `ReportPrinter{Report}` is the only thing that formats reports, rendering each result's `Summary` beside its name. The `Report` struct itself stays pure data.
- `builder.go` — `Builder` struct (returns `[]Check` + optional cleanup), `Builders` registry slice, per-category builder functions (`BuildConnectivityChecks`, …), `BuildChecks(cfg, selected, opts...)`. A builder takes `(*stream.Config, ...ConnOption)`: the config says which checks apply, the options configure every connection those checks open. `ConnOption` is driver-neutral — `WithDialFunc` and `WithLookupFunc` take stdlib types, and `postgresConnOptions` renders them for `internal/postgres`, so pgx stays out of the package's public API.
- One file per category of concrete checks (`connectivity.go`, `replication.go`, …).

The shared-conn primitive lives one floor down at `internal/postgres.LazyConn` so other callers can reuse it.

## Adding a new check

Adding a check is meant to be a small, mechanical edit. Keep it that way.

1. **Pick a category.** Categories group checks of the same concern (`connectivity`, `replication`, `access`, `schema`, `resources`).
   - Joining an existing category: skip to step 2.
   - Creating a new one: add a `Category` constant in `preflight.go`, a builder func (`func(*stream.Config, ...ConnOption) ([]Check, CleanupFunc)`) + `Builders` entry in `builder.go`, and a boolean flag on `checkCmd` in `cmd/root_cmd.go`. The flag string must match `Builder.Flag`.
2. **Implement the check.** New struct in `<thing>.go`, satisfying the `Check` interface.
   - **Every `Finding` is blocking.** A check that finds nothing wrong returns a `nil` slice.
   - **Return `error` only when the check itself couldn't run** (timeout, internal bug, malformed input). A detected problem is a `Finding`, not an error.
   - **Every finding declares an `ID` and a `Title`** — see [Identifying a finding](#identifying-a-finding).
   - **Put remediation in `Finding.Message`** — the user should be able to act on it without reading source.
3. **Report what it observed**, if it observed anything worth reporting — see [Reporting what a check observed](#reporting-what-a-check-observed). Most checks need none of this: a check that only passes or fails implements no optional interface.
4. **Materialise instances in the category builder** (e.g. `BuildConnectivityChecks`). The builder is the applicability gate: it reads `*stream.Config` and decides which instances are relevant. Inapplicable checks are silently omitted today; an explicit "skipped: <reason>" mechanism is deferred (see `docs/migration_preflight_issue.md` "Architecture decisions" #6).
   - **If checks in the category share a Postgres connection**, call `postgres.NewLazyConn(url, postgresConnOptions(opts)...)` in the builder, hand `src.Acquire` (a `postgres.AcquireFunc`) to every check, and return `src.Close` as the cleanup. See `BuildReplicationChecks` for the pattern. The engine runs sequentially, so the first check to call `Source(ctx)` opens the conn and the rest reuse it. A failed dial is memoised too — only one connection attempt happens, even if every check reports its own check error.
   - **Every connection must carry the connection options.** Forwarding them is what lets a library caller decide which address a check reaches, and the guarantee only holds while every builder does it. A check that opens its own connection instead of sharing one takes a `ConnOptions []ConnOption` field, set from the builder's `opts`, and passes `postgresConnOptions(c.ConnOptions)...` to `postgres.NewConn` — see `ConnectivityCheck`. `TestBuildSourceChecks_ConnOptions` fails when a connection escapes the options, so add the new category's URLs to the configuration it builds.
5. **Tests.** Unit-test the check directly against mocked dependencies (`internal/postgres/mocks` has the postgres conn mock). For new categories, exercise the builder selection path through the cmd layer too.

## Identifying a finding

`Finding` carries four fields. A construction site sets all four; a test reads the package source and fails a site that omits one.

| field | contains | consumed by |
| --- | --- | --- |
| `ID` | a stable slug naming the *kind* of problem | metrics keys, consumer copy |
| `Title` | one short line naming the problem | a heading in a consumer's UI |
| `Detail` | the specifics: the tables, the version, the setting value | a body in a consumer's UI |
| `Message` | the single line the CLI prints | `pgstream check` |

- **Declare the id as a constant in `finding_ids.go`.** `ID` names that constant; it is never a computed string. One id names exactly one construction site, and a test enforces both rules.
- **An id is stable.** Once released it does not change, because consumers key metrics and their own copy on it. Ids are lowercase letters, digits and underscores.
- **`ID` and `Title` never carry data read from the database under test.** Instance specifics — schema, table, column, role, version, setting value — go in `Detail` and `Message` only. One kind of problem found on twenty tables is one finding with twenty tables in `Detail`. A test enforces this by shape: `ID` must name a constant and `Title` must be a plain string literal, so neither can interpolate a value.
- **A check that reports several kinds of problem declares an id per kind.** `assessReplicaIdentity` is the pattern: one helper, four ids.
- **`Message` keeps the CLI output stable.** The printer renders `Message` alone, so changing it changes what a user reads.

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
