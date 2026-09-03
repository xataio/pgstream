// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"

	"github.com/xataio/pgstream/internal/postgres"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/stream"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

// CleanupFunc releases any resources a builder set up (e.g. a shared Postgres
// connection). Builders return nil when there's nothing to clean up.
type CleanupFunc func(context.Context) error

// Builder turns a stream.Config into the concrete checks for a category, plus
// an optional cleanup function that releases resources the checks share (e.g.
// a Postgres connection). Each new category adds an entry to Builders and a
// matching CLI flag in cmd/root_cmd.go.
type Builder struct {
	Category Category
	Flag     string
	Build    func(*stream.Config) ([]Check, CleanupFunc)
}

// Builders is the registry of category builders. Adding a new category = one
// Builder entry here + one flag declaration on checkCmd.
var Builders = []Builder{
	{CategoryConnectivity, "connectivity", BuildConnectivityChecks},
	{CategoryReplication, "replication", BuildReplicationChecks},
	{CategoryAccess, "access", BuildAccessChecks},
	{CategorySchema, "schema", BuildSchemaChecks},
	{CategoryResources, "resources", BuildResourcesChecks},
}

// sourceChecksReplicationSlot is the slot name BuildSourceChecks uses.
const sourceChecksReplicationSlot = "pgstream_preflight_source_checks"

// SourceOption configures BuildSourceChecks.
type SourceOption func(*sourceOptions)

type sourceOptions struct {
	replicationSlot string
	snapshotData    *pgsnapshotgenerator.Config
	categories      []Category
}

// WithSourceCategories restricts the run to the given categories, in the order
// they are registered in Builders. Omitting it runs every category.
func WithSourceCategories(categories ...Category) SourceOption {
	return func(o *sourceOptions) { o.categories = categories }
}

// BuildSourceChecks returns every preflight check that only needs a connection
// to the source Postgres, plus a cleanup function releasing the connections
// those checks share. It is the entry point for callers that want to validate a
// source without assembling a full stream.Config: connectivity, replication
// readiness, source read privileges, schema compatibility and snapshot capacity.
//
// Checks that compare the source against a target (extension compatibility,
// range-type support, the target privilege checks) are excluded by
// construction, and postgres_version reports the source version alone.
//
// The returned cleanup is always non-nil, including on error, so callers can
// defer it unconditionally.
func BuildSourceChecks(sourceURL string, opts ...SourceOption) ([]Check, CleanupFunc, error) {
	if sourceURL == "" {
		return nil, joinCleanups(nil), errors.New("source postgres url is required")
	}

	o := sourceOptions{
		replicationSlot: sourceChecksReplicationSlot,
		snapshotData:    &pgsnapshotgenerator.Config{},
	}
	for _, opt := range opts {
		opt(&o)
	}

	checks, cleanup := BuildChecks(o.streamConfig(sourceURL), o.categories)
	return checks, cleanup, nil
}

// streamConfig synthesises the source-only stream.Config the category builders
// consume, so BuildSourceChecks picks up new source checks without a second
// registration point. No table scope is configured, so every check that
// inspects user tables covers the whole database.
func (o *sourceOptions) streamConfig(sourceURL string) *stream.Config {
	return &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: sourceURL,
				Replication: pgreplication.Config{
					ReplicationSlotName: o.replicationSlot,
				},
				// Data carries the snapshot sizing the snapshot-only checks read,
				// and its presence is the gate those checks use.
				Snapshot: &snapshotbuilder.SnapshotListenerConfig{
					Data: o.snapshotData,
				},
			},
		},
	}
}

// BuildResourcesChecks returns the resource-capacity preflight checks that
// apply to cfg, plus a cleanup function that closes the shared source
// connection. The database-size report applies to any configured source. The
// snapshot connection-headroom check is added only when a data snapshot is
// configured, because it sizes snapshot_workers x table_workers against the
// source's max_connections.
func BuildResourcesChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	url := cfg.SourcePostgresURL()
	if url == "" {
		return nil, nil
	}
	src := postgres.NewLazyConn(url)
	checks := []Check{
		&DatabaseSizeCheck{Source: src.Acquire},
	}
	if demand, ok := cfg.SnapshotConnectionDemand(); ok {
		checks = append(checks, &SnapshotConnectionsCheck{Source: src.Acquire, Demand: demand})
	}
	return checks, src.Close
}

// BuildConnectivityChecks returns the connectivity checks applicable to cfg.
// A source check is added when a source postgres URL is configured; a target
// check is added when a postgres target is configured. Each check opens its
// own conn (to its own URL), so no shared cleanup is needed.
func BuildConnectivityChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	checks := []Check{}
	if url := cfg.SourcePostgresURL(); url != "" {
		checks = append(checks, &ConnectivityCheck{Label: "source", URL: url})
		if demand, ok := cfg.SnapshotConnectionDemand(); ok {
			checks = append(checks, &SourceSnapshotInstanceCheck{
				Probe: func(ctx context.Context, probes int) (int, error) {
					return postgres.ProbeExportedSnapshotVisibility(ctx, func(ctx context.Context) (postgres.Querier, error) {
						return postgres.NewConn(ctx, url)
					}, probes)
				},
				Probes: snapshotInstanceProbes(demand),
			})
		}
	}
	if cfg.Processor.Postgres != nil {
		if url := cfg.Processor.Postgres.BatchWriter.URL; url != "" {
			checks = append(checks, &ConnectivityCheck{Label: "target", URL: url})
		}
	}
	return checks, nil
}

func snapshotInstanceProbes(demand uint) int {
	const minProbes, maxProbes = 4, 16
	switch {
	case demand < minProbes:
		return minProbes
	case demand > maxProbes:
		return maxProbes
	default:
		return int(demand)
	}
}

// BuildReplicationChecks returns the replication-preflight checks applicable
// to cfg, plus a cleanup function that closes the shared source connection.
// Replication checks only apply when the source is configured with a
// replication slot.
func BuildReplicationChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	if cfg.PostgresReplicationSlot() == "" {
		return nil, nil
	}
	url := cfg.SourcePostgresURL()
	if url == "" {
		return nil, nil
	}
	src := postgres.NewLazyConn(url)
	return []Check{
		&WALLevelCheck{Source: src.Acquire},
		&WAL2JSONCheck{Source: src.Acquire},
		&ReplicationSlotHeadroomCheck{Source: src.Acquire},
		&ReplicationRoleAttrCheck{Source: src.Acquire},
		&ReplicaIdentityCheck{Source: src.Acquire, Selection: cfg.ReplicationTableSelection()},
	}, src.Close
}

// BuildAccessChecks returns the access-preflight checks applicable to cfg,
// plus a cleanup function that closes the shared source connection.
func BuildAccessChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	sourceURL := cfg.SourcePostgresURL()
	if sourceURL == "" {
		return nil, nil
	}
	src := postgres.NewLazyConn(sourceURL)
	selection := cfg.AccessTableSelection()
	checks := []Check{
		&SourceTableSelectPrivilegesCheck{
			Source:    src.Acquire,
			Selection: selection,
		},
		&SourceSequenceSelectPrivilegesCheck{
			Source:    src.Acquire,
			Selection: selection,
		},
	}

	cleanups := []CleanupFunc{src.Close}

	createDB, restoreRoles := cfg.SnapshotCreateTargetDB(), cfg.SnapshotRestoresRoles()
	if targetURL := cfg.SnapshotTargetPostgresURL(); targetURL != "" && (createDB || restoreRoles) {
		// both checks ask the same cluster the same kind of question, so they
		// resolve one connection string and share one connection
		checkURL, err := targetPrivilegeCheckURL(targetURL, createDB)
		acquire := func(context.Context) (postgres.Querier, error) { return nil, err }
		if err == nil {
			target := postgres.NewLazyConn(checkURL)
			acquire = target.Acquire
			cleanups = append(cleanups, target.Close)
		}
		if createDB {
			checks = append(checks, &TargetCreateDBPrivilegeCheck{Target: acquire})
		}
		if restoreRoles {
			checks = append(checks, &TargetCreateRolePrivilegeCheck{Target: acquire})
		}
	}

	return checks, joinCleanups(cleanups)
}

// targetPrivilegeCheckURL returns the connection string the target privilege
// checks connect to. CREATEDB and CREATEROLE are cluster wide role attributes,
// so any database in the target cluster can answer the question. The
// configured target database is only dropped from the connection string when
// the snapshot creates it, since it does not exist yet at check time.
func targetPrivilegeCheckURL(targetURL string, createTargetDB bool) (string, error) {
	if !createTargetDB {
		return targetURL, nil
	}
	return postgres.RemoveDatabaseFromConnectionString(targetURL)
}

// BuildSchemaChecks returns the schema-preflight checks applicable to cfg,
// plus a cleanup function that closes the shared source (and, when the target
// is Postgres, target) connection. Schema checks cover every table pgstream
// reads (snapshot and replication), so they use the combined access table
// selection. The version check runs whenever a source is configured — reporting
// the source version alone (so it survives --source) and additionally comparing
// against the target when a Postgres target URL is configured. The range-type
// check is added when the target is Postgres; the extension check additionally
// needs the target URL to query the target.
func BuildSchemaChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	url := cfg.SourcePostgresURL()
	if url == "" {
		return nil, nil
	}
	src := postgres.NewLazyConn(url)
	selection := cfg.AccessTableSelection()
	versionCheck := &PostgresVersionCheck{Source: src.Acquire}
	checks := []Check{
		versionCheck,
		&SchemaTypeCompatibilityCheck{
			Source:    src.Acquire,
			Selection: selection,
		},
	}
	cleanups := []CleanupFunc{src.Close}
	if cfg.Processor.Postgres != nil {
		checks = append(checks, &PostgresRangeTypeCheck{
			Source:    src.Acquire,
			Selection: selection,
		})
		if targetURL := cfg.Processor.Postgres.BatchWriter.URL; targetURL != "" {
			tgt := postgres.NewLazyConn(targetURL)
			cleanups = append(cleanups, tgt.Close)
			versionCheck.Target = tgt.Acquire
			checks = append(checks, &SchemaExtensionCompatibilityCheck{
				Source: src.Acquire,
				Target: tgt.Acquire,
			})
		}
	}
	return checks, joinCleanups(cleanups)
}

// joinCleanups returns a single CleanupFunc that runs each cleanup in order and
// returns the first error encountered (still running the rest).
func joinCleanups(cleanups []CleanupFunc) CleanupFunc {
	return func(ctx context.Context) error {
		var firstErr error
		for _, c := range cleanups {
			if err := c(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}

// BuildChecks returns the concrete checks for the selected categories,
// preserving the registration order in Builders, plus a single cleanup
// function that releases every category's resources. The returned cleanup is
// always non-nil; callers can defer it unconditionally. An empty selection
// runs every registered category.
func BuildChecks(cfg *stream.Config, selected []Category) ([]Check, CleanupFunc) {
	want := make(map[Category]bool, len(selected))
	for _, c := range selected {
		want[c] = true
	}
	checks := []Check{}
	cleanups := []CleanupFunc{}
	for _, b := range Builders {
		if len(want) == 0 || want[b.Category] {
			cs, cleanup := b.Build(cfg)
			checks = append(checks, cs...)
			if cleanup != nil {
				cleanups = append(cleanups, cleanup)
			}
		}
	}
	return checks, joinCleanups(cleanups)
}
