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
	logger loglib.Logger
	pgConn pglib.Querier
	// generatedTableColumns is a map of schema.table to a list of generated column names.
	generatedTableColumns *synclib.Map[string, map[string]struct{}]
	// materializedViews is a map of schema name to a set of materialized view names.
	materializedViews *synclib.Map[string, map[string]struct{}]
	// columnTableSequences is a map of schema.table to a map of sequence column names.
	columnTableSequences *synclib.Map[string, map[string]string]
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
		generatedTableColumns: synclib.NewMap[string, map[string]struct{}](),
		materializedViews:     synclib.NewMap[string, map[string]struct{}](),
		columnTableSequences:  synclib.NewMap[string, map[string]string](),
		logger:                logger,
	}, nil
}

// getGeneratedColumnNames will return a list of generated column names for the
// schema.table on input. If the value is not in the internal cache, it will
// query postgres.
func (o *pgSchemaObserver) getGeneratedColumnNames(ctx context.Context, schema, table string) (map[string]struct{}, error) {
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

func (o *pgSchemaObserver) getSequenceColumns(ctx context.Context, schema, table string) (map[string]string, error) {
	key := pglib.QuoteQualifiedIdentifier(schema, table)
	colSeqMap, found := o.columnTableSequences.Get(key)
	if found {
		return colSeqMap, nil
	}

	// if not found in the map, retrieve them from postgres
	seqColMap, err := o.queryTableSequences(ctx, o.pgConn, schema, table)
	if err != nil {
		o.logger.Error(err, "querying column sequences from postgres", loglib.Fields{"schema": schema, "table": table})
		return nil, err
	}

	o.columnTableSequences.Set(key, seqColMap)
	return seqColMap, nil
}

func (o *pgSchemaObserver) update(logEntry *schemalog.LogEntry) {
	o.updateGeneratedColumnNames(logEntry)
	o.updateMaterializedViews(logEntry)
	o.updateColumnSequences(logEntry)
}

// updateGeneratedColumnNames will update the internal cache with the table
// columns for the schema log on input.
func (o *pgSchemaObserver) updateGeneratedColumnNames(logEntry *schemalog.LogEntry) {
	for _, table := range logEntry.Schema.Tables {
		key := pglib.QuoteQualifiedIdentifier(logEntry.SchemaName, table.Name)
		generatedColumns := make(map[string]struct{}, len(table.Columns))
		for _, c := range table.Columns {
			if c.IsGenerated() {
				generatedColumns[pglib.QuoteIdentifier(c.Name)] = struct{}{}
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

func (o *pgSchemaObserver) updateColumnSequences(logEntry *schemalog.LogEntry) {
	for _, table := range logEntry.Schema.Tables {
		key := pglib.QuoteQualifiedIdentifier(logEntry.SchemaName, table.Name)
		seqColMap := make(map[string]string)
		for _, col := range table.Columns {
			if col.HasSequence() {
				seqColMap[pglib.QuoteIdentifier(col.Name)] = col.GetSequenceName()
			}
		}
		o.columnTableSequences.Set(key, seqColMap)
	}
}

const generatedTableColumnsQuery = `SELECT attname FROM pg_attribute
		WHERE attnum > 0
		AND attrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2)
		AND (attgenerated != '' OR attidentity != '')`

func (o *pgSchemaObserver) queryGeneratedColumnNames(ctx context.Context, schemaName, tableName string) (map[string]struct{}, error) {
	columnNames := map[string]struct{}{}
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
		columnNames[pglib.QuoteIdentifier(columnName)] = struct{}{}
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

const sequenceColumnQuery = `SELECT
    a.attname AS column_name,
    s.relname AS sequence_name
FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_attribute a ON a.attrelid = t.oid
JOIN pg_attrdef ad ON ad.adrelid = t.oid AND ad.adnum = a.attnum
JOIN pg_depend d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
JOIN pg_class s ON s.oid = d.objid
WHERE t.relkind = 'r'
    AND s.relkind = 'S'
    AND d.deptype = 'a'
    AND n.nspname = $1
    AND t.relname = $2
    AND a.attnum > 0
    AND NOT a.attisdropped;`

func (o *pgSchemaObserver) queryTableSequences(ctx context.Context, conn pglib.Querier, schemaName, tableName string) (map[string]string, error) {
	rows, err := conn.Query(ctx, sequenceColumnQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting sequences for table %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	seqColMap := make(map[string]string)
	for rows.Next() {
		var columnName, sequenceName string
		if err := rows.Scan(&columnName, &sequenceName); err != nil {
			return nil, fmt.Errorf("scanning sequence column mapping: %w", err)
		}
		seqColMap[pglib.QuoteIdentifier(columnName)] = pglib.QuoteQualifiedIdentifier(schemaName, sequenceName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return seqColMap, nil
}

func (o *pgSchemaObserver) close() error {
	return o.pgConn.Close(context.Background())
}
