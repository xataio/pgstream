// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"

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
}

// BuildConnectivityChecks returns the connectivity checks applicable to cfg.
// A source check is added when a source postgres URL is configured; a target
// check is added when a postgres target is configured. Each check opens its
// own conn (to its own URL), so no shared cleanup is needed.
func BuildConnectivityChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	checks := []Check{}
	if url := cfg.SourcePostgresURL(); url != "" {
		checks = append(checks, &ConnectivityCheck{Label: "source", URL: url})
	}
	if cfg.Processor.Postgres != nil {
		if url := cfg.Processor.Postgres.BatchWriter.URL; url != "" {
			checks = append(checks, &ConnectivityCheck{Label: "target", URL: url})
		}
	}
	return checks, nil
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
	}, src.Close
}

// BuildAccessChecks returns the access-preflight checks applicable to cfg,
// plus a cleanup function that closes the shared source connection.
func BuildAccessChecks(cfg *stream.Config) ([]Check, CleanupFunc) {
	url := cfg.SourcePostgresURL()
	if url == "" {
		return nil, nil
	}
	src := postgres.NewLazyConn(url)
	check := &SourceTableSelectPrivilegesCheck{Source: src.Acquire}
	if cfg.Listener.Postgres != nil && cfg.Listener.Postgres.Snapshot != nil {
		check.Tables = cfg.Listener.Postgres.Snapshot.Adapter.Tables
		check.ExcludedTables = cfg.Listener.Postgres.Snapshot.Adapter.ExcludedTables
	}
	return []Check{check}, src.Close
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
	return checks, func(ctx context.Context) error {
		var firstErr error
		for _, c := range cleanups {
			if err := c(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}
