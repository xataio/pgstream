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
		schemaDiscoveryFn: discoverAllSchemas,
		tableDiscoveryFn:  discoverAllSchemaTables,
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
	_, wildcardSchemaFound := ss.SchemaTables[wildcard]
	if wildcardSchemaFound {
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
	return s.wrapped.CreateSnapshot(ctx, ss)
}

func (s *SnapshotSchemaTableFinder) Close() error {
	return s.conn.Close(context.Background())
}

func discoverAllSchemaTables(ctx context.Context, conn pglib.Querier, schema string) ([]string, error) {
	const query = "SELECT tablename FROM pg_tables WHERE schemaname=$1"
	rows, err := conn.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("discovering all tables for schema %s: %w", schema, err)
	}
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func discoverAllSchemas(ctx context.Context, conn pglib.Querier) ([]string, error) {
	const query = "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pgstream')"
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("discovering all schemas for wildcard: %w", err)
	}
	defer rows.Close()

	schemas := []string{}
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return nil, fmt.Errorf("scanning schema name: %w", err)
		}
		schemas = append(schemas, schemaName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return schemas, nil
}
