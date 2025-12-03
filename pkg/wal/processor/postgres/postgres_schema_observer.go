// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
)

// pgSchemaObserver keeps track of schema metadata including generated column
// names and materialized views for tables. It uses a cache to reduce the number
// of calls to postgres, and it updates the state whenever a DDL event is
// received through the WAL.
type pgSchemaObserver struct {
	logger                loglib.Logger
	pgConn                pglib.Querier
	generatedTableColumns *synclib.Map[string, []string]
	// materializedViews is a map of schema name to a set of materialized view names.
	materializedViews *synclib.Map[string, map[string]struct{}]
}

// newPGSchemaObserver returns a postgres observer that tracks schemas,
// including generated table columns and materialized views. It keeps a cache to
// reduce the number of calls to postgres, and it updates the state whenever a
// DDL event is received through the WAL.
func newPGSchemaObserver(ctx context.Context, pgURL string, logger loglib.Logger) (*pgSchemaObserver, error) {
	pgConn, err := pglib.NewConnPool(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	return &pgSchemaObserver{
		pgConn:                pgConn,
		generatedTableColumns: synclib.NewMap[string, []string](),
		materializedViews:     synclib.NewMap[string, map[string]struct{}](),
		logger:                logger,
	}, nil
}

// getGeneratedColumnNames will return a list of generated column names for the
// schema.table on input. If the value is not in the internal cache, it will
// query postgres.
func (o *pgSchemaObserver) getGeneratedColumnNames(ctx context.Context, schema, table string) ([]string, error) {
	key := pglib.QuoteQualifiedIdentifier(schema, table)

	columns, found := o.generatedTableColumns.Get(key)
	if found {
		return columns, nil
	}

	//  if not found in the map, retrieve them from postgres
	colNames, err := o.queryGeneratedColumnNames(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	o.generatedTableColumns.Set(key, colNames)
	return colNames, nil
}

// isMaterializedView will return true if the input schema.table is a
// materialized view. It uses an internal cache to reduce the number of calls to
// postgres. If the value is not in the cache, it will query postgres.
func (o *pgSchemaObserver) isMaterializedView(ctx context.Context, schema, table string) bool {
	key := pglib.QuoteIdentifier(schema)
	materializedViews, found := o.materializedViews.Get(key)
	if found {
		_, found := materializedViews[pglib.QuoteIdentifier(table)]
		return found
	}

	// if not found in the map, retrieve them from postgres
	mvNames, err := o.queryMaterializedViews(ctx, schema)
	if err != nil {
		o.logger.Error(err, "querying materialized views from postgres", loglib.Fields{"schema": schema})
		return false
	}

	o.materializedViews.Set(key, mvNames)
	_, found = mvNames[pglib.QuoteIdentifier(table)]
	return found
}

// updateGeneratedColumnNames will update the internal cache with the table
// columns for the schema log on input.
func (o *pgSchemaObserver) updateGeneratedColumnNames(logEntry *schemalog.LogEntry) {
	for _, table := range logEntry.Schema.Tables {
		key := pglib.QuoteQualifiedIdentifier(logEntry.SchemaName, table.Name)
		generatedColumns := make([]string, 0, len(table.Columns))
		for _, c := range table.Columns {
			if c.IsGenerated() {
				generatedColumns = append(generatedColumns, c.Name)
			}
		}

		o.generatedTableColumns.Set(key, generatedColumns)
	}
}

// updateMaterializedViews will update the internal cache with the materialized
// views for the schema log on input.
func (o *pgSchemaObserver) updateMaterializedViews(logEntry *schemalog.LogEntry) {
	key := pglib.QuoteIdentifier(logEntry.SchemaName)
	mvNames := make(map[string]struct{}, len(logEntry.Schema.MaterializedViews))
	for _, mv := range logEntry.Schema.MaterializedViews {
		mvNames[pglib.QuoteIdentifier(mv.Name)] = struct{}{}
	}
	o.materializedViews.Set(key, mvNames)
}

const generatedTableColumnsQuery = `SELECT attname FROM pg_attribute
		WHERE attnum > 0
		AND attrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2)
		AND (attgenerated != '' OR attidentity != '')`

func (o *pgSchemaObserver) queryGeneratedColumnNames(ctx context.Context, schemaName, tableName string) ([]string, error) {
	columnNames := []string{}
	// filter out generated columns (excluding identities) since they will
	// be generated automatically, and they can't be overwriten.
	rows, err := o.pgConn.Query(ctx, generatedTableColumnsQuery, tableName, schemaName)
	if err != nil {
		return nil, fmt.Errorf("getting table generated column names for table %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scanning table generated column name: %w", err)
		}
		columnNames = append(columnNames, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columnNames, nil
}

const materializedViewsQuery = `SELECT matviewname FROM pg_matviews WHERE schemaname = $1`

func (o *pgSchemaObserver) queryMaterializedViews(ctx context.Context, schemaName string) (map[string]struct{}, error) {
	mvNames := make(map[string]struct{})
	rows, err := o.pgConn.Query(ctx, materializedViewsQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("getting materialized views for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var mvName string
		if err := rows.Scan(&mvName); err != nil {
			return nil, fmt.Errorf("scanning materialized view name: %w", err)
		}
		mvNames[pglib.QuoteIdentifier(mvName)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return mvNames, nil
}

func (o *pgSchemaObserver) close() error {
	return o.pgConn.Close(context.Background())
}
