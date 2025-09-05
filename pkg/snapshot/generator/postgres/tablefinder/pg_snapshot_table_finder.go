// SPDX-License-Identifier: Apache-2.0

package tablefinder

import (
	"context"
	"fmt"
	"slices"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
)

// SnapshotSchemaTableFinder is a decorator around a snapshot generator that will
// explode the wildcard references in the snapshot requests and replace them by
// all the schemas & schema tables in postgres.
type SnapshotSchemaTableFinder struct {
	wrapped           generator.SnapshotGenerator
	conn              pglib.Querier
	tableDiscoveryFn  tableDiscoveryFn
	schemaDiscoveryFn schemaDiscoveryFn
}

type Option func(*SnapshotSchemaTableFinder)

type schemaDiscoveryFn func(ctx context.Context, conn pglib.Querier) ([]string, error)

type tableDiscoveryFn func(ctx context.Context, conn pglib.Querier, schema string) ([]string, error)

const wildcard = "*"

// NewSnapshotSchemaTableFinder will return the generator on input wrapped with a
// schema & table finder that will explode the wildcard references in the snapshot
// request and translate them into all the postgres tables for the given schema,
// or all tables of all schemas in case of '*.*'.
func NewSnapshotSchemaTableFinder(ctx context.Context, pgurl string, generator generator.SnapshotGenerator, opts ...Option) (*SnapshotSchemaTableFinder, error) {
	conn, err := pglib.NewConnPool(ctx, pgurl)
	if err != nil {
		return nil, err
	}

	stf := &SnapshotSchemaTableFinder{
		wrapped:           generator,
		conn:              conn,
		schemaDiscoveryFn: pglib.DiscoverAllSchemas,
		tableDiscoveryFn:  pglib.DiscoverAllSchemaTables,
	}

	for _, opt := range opts {
		opt(stf)
	}

	return stf, nil
}

func WithInstrumentation(i *otel.Instrumentation) Option {
	return func(stf *SnapshotSchemaTableFinder) {
		var err error
		stf.conn, err = pglibinstrumentation.NewQuerier(stf.conn, i)
		if err != nil {
			panic(err)
		}

		stf.schemaDiscoveryFn, stf.tableDiscoveryFn = newInstrumentedSchemaTableDiscoveryFns(stf.schemaDiscoveryFn, stf.tableDiscoveryFn, i)
	}
}

func (s *SnapshotSchemaTableFinder) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	tablenames, wildcardSchemaFound := ss.SchemaTables[wildcard]
	if wildcardSchemaFound {
		if len(tablenames) != 1 || tablenames[0] != wildcard {
			return fmt.Errorf("wildcard schema must be used with wildcard table, got %q", tablenames)
		}

		schemas, err := s.schemaDiscoveryFn(ctx, s.conn)
		if err != nil {
			return err
		}
		for _, schema := range schemas {
			ss.SchemaTables[schema] = []string{wildcard}
		}
		delete(ss.SchemaTables, wildcard)
	}

	for schema, tables := range ss.SchemaTables {
		if slices.Contains(tables, wildcard) {
			var err error
			ss.SchemaTables[schema], err = s.tableDiscoveryFn(ctx, s.conn, schema)
			if err != nil {
				return err
			}
		}
	}

	if len(ss.SchemaExcludedTables) == 0 {
		// No excluded tables, return early
		return s.wrapped.CreateSnapshot(ctx, ss)
	}

	// Remove excluded tables from the snapshot request
	for schema, tables := range ss.SchemaTables {
		if excludedTables, found := ss.SchemaExcludedTables[schema]; found {
			// Filter out the excluded tables
			filteredTables := []string{}
			for _, table := range tables {
				if !slices.Contains(excludedTables, table) {
					filteredTables = append(filteredTables, table)
				}
			}
			if len(filteredTables) == 0 {
				// If no tables left after filtering, remove the schema from the snapshot
				delete(ss.SchemaTables, schema)
			} else {
				ss.SchemaTables[schema] = filteredTables
			}
		}
	}

	return s.wrapped.CreateSnapshot(ctx, ss)
}

func (s *SnapshotSchemaTableFinder) Close() error {
	return s.conn.Close(context.Background())
}
