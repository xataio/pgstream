// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"strings"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
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

// BuildResourcesChecks returns the resource-capacity preflight checks
// applicable to cfg, plus a cleanup function that closes the shared source
// connection. The snapshot connection-headroom check only applies when a data
// snapshot is configured (it sizes snapshot_workers × table_workers against the
// source's max_connections).
func BuildResourcesChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	url := cfg.SourcePostgresURL()
	if url == "" {
		return nil, nil
	}
	demand, ok := cfg.SnapshotConnectionDemand()
	if !ok {
		return nil, nil
	}
	src := postgres.NewLazyConn(url)
	return []Check{
		&SnapshotConnectionsCheck{Source: src.Acquire, Demand: demand},
	}, src.Close
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

	if cfg.SnapshotCreateTargetDB() {
		if targetURL := cfg.SnapshotTargetPostgresURL(); targetURL != "" {
			var err error
			targetURL, err = removeDatabaseFromConnectionString(targetURL)
			if err != nil {
				checks = append(checks, &TargetCreateDBPrivilegeCheck{
					Target: func(context.Context) (postgres.Querier, error) {
						return nil, err
					},
				})
				return checks, joinCleanups(cleanups)
			}
			target := postgres.NewLazyConn(targetURL)
			checks = append(checks, &TargetCreateDBPrivilegeCheck{Target: target.Acquire})
			cleanups = append(cleanups, target.Close)
		}
	}

	return checks, func(ctx context.Context) error {
		var firstErr error
		for _, cleanup := range cleanups {
			if err := cleanup(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}

// removeDatabaseFromConnectionString lets the CREATEDB preflight connect to
// the target server before the configured target database exists.
func removeDatabaseFromConnectionString(url string) (string, error) {
	pgCfg, err := postgres.ParseConfig(url)
	if err != nil {
		return "", err
	}
	if pgCfg.Database == "" || pgCfg.Database == "postgres" {
		return url, nil
	}

	return strings.ReplaceAll(url, "/"+pgCfg.Database, "/"), nil
}

// BuildSchemaChecks returns the schema-preflight checks applicable to cfg,
// plus a cleanup function that closes the shared source (and, when the target
// is Postgres, target) connection. Schema checks cover every table pgstream
// reads (snapshot and replication), so they use the combined access table
// selection. The range-type and extension checks are target specific and are
// only added when the target is Postgres.
func BuildSchemaChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	url := cfg.SourcePostgresURL()
	if url == "" {
		return nil, nil
	}
	src := postgres.NewLazyConn(url)
	selection := cfg.AccessTableSelection()
	checks := []Check{
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
