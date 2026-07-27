// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibretrier "github.com/xataio/pgstream/internal/postgres/retrier"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

// schemaQueryTimeout bounds each catalog lookup. Without it a hung target
// blocks the listener goroutine indefinitely, which stops standby status
// updates and makes the source retain WAL until its disk fills.
const schemaQueryTimeout = 30 * time.Second

// pgSchemaObserver keeps track of schema metadata including generated column
// names and materialized views for tables. It uses a cache to reduce the number
// of calls to postgres, and it updates the state whenever a DDL event is
// received through the WAL.
type pgSchemaObserver struct {
	logger loglib.Logger
	pgConn pglib.Querier
	// generatedTableColumns is a map of schema.table to a list of generated column names.
	generatedTableColumns *synclib.Map[string, map[string]struct{}]
	// alwaysIdentityTableColumns is a map of schema.table to a set of column names
	// defined as GENERATED ALWAYS AS IDENTITY. These must be filtered from UPDATE
	// SET clauses since Postgres rejects explicit values for them.
	alwaysIdentityTableColumns *synclib.Map[string, map[string]struct{}]
	// materializedViews is a map of schema name to a set of materialized view names.
	materializedViews *synclib.Map[string, map[string]struct{}]
	// columnTableSequences is a map of schema.table to a map of sequence column names.
	columnTableSequences *synclib.Map[string, map[string]string]
	// enumTableColumns is a map of schema.table to a set of quoted column names
	// whose type is a user-defined enum.
	enumTableColumns *synclib.Map[string, map[string]struct{}]
	// enumCacheEpoch is bumped by every enum invalidation. Unlike its sibling
	// caches, enumTableColumns is repopulated by a live query rather than from
	// the DDL event payload, so a lookup that started before an invalidation
	// could otherwise write its stale answer back and outlive the eviction
	// meant to drop it. Lookups record the epoch before querying and only
	// cache the result if it has not moved.
	enumCacheEpoch atomic.Uint64
}

// newPGSchemaObserver returns a postgres observer that tracks schemas,
// including generated table columns and materialized views. It keeps a cache to
// reduce the number of calls to postgres, and it updates the state whenever a
// DDL event is received through the WAL.
func newPGSchemaObserver(ctx context.Context, cfg *Config, logger loglib.Logger) (*pgSchemaObserver, error) {
	newConnPool := func(ctx context.Context) (pglib.Querier, error) {
		return pglib.NewConnPool(ctx, cfg.URL)
	}

	// the observer sits on the hot path: an unretried transient failure here
	// propagates all the way up and terminates the pipeline, so use the same
	// retry policy as the writer unless retries are disabled.
	var pgConn pglib.Querier
	var err error
	if cfg.RetryPolicy.DisableRetries {
		pgConn, err = newConnPool(ctx)
	} else {
		pgConn, err = pglibretrier.NewQuerier(ctx, cfg.retryPolicy(), newConnPool, logger)
	}
	if err != nil {
		return nil, err
	}

	return &pgSchemaObserver{
		pgConn:                     pgConn,
		generatedTableColumns:      synclib.NewMap[string, map[string]struct{}](),
		alwaysIdentityTableColumns: synclib.NewMap[string, map[string]struct{}](),
		materializedViews:          synclib.NewMap[string, map[string]struct{}](),
		columnTableSequences:       synclib.NewMap[string, map[string]string](),
		enumTableColumns:           synclib.NewMap[string, map[string]struct{}](),
		logger:                     logger,
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

// getAlwaysIdentityColumnNames returns the set of GENERATED ALWAYS AS IDENTITY
// column names for the given schema.table. If not cached, it queries postgres.
func (o *pgSchemaObserver) getAlwaysIdentityColumnNames(ctx context.Context, schema, table string) (map[string]struct{}, error) {
	key := pglib.QuoteQualifiedIdentifier(schema, table)

	columns, found := o.alwaysIdentityTableColumns.Get(key)
	if found {
		return columns, nil
	}

	colNames, err := o.queryAlwaysIdentityColumnNames(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	o.alwaysIdentityTableColumns.Set(key, colNames)
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

// getEnumColumnNames returns the set of quoted column names for the given
// schema.table whose type is a user-defined enum. If not cached, it queries
// postgres.
func (o *pgSchemaObserver) getEnumColumnNames(ctx context.Context, schema, table string) (map[string]struct{}, error) {
	key := pglib.QuoteQualifiedIdentifier(schema, table)

	columns, found := o.enumTableColumns.Get(key)
	if found {
		return columns, nil
	}

	epoch := o.enumCacheEpoch.Load()
	colNames, err := o.queryEnumColumnNames(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	// drop the result instead of caching it if a DDL invalidated enum state
	// while the query was in flight: it may predate the DDL, and caching it
	// would survive the eviction that was meant to remove it.
	if o.enumCacheEpoch.Load() == epoch {
		o.enumTableColumns.Set(key, colNames)
	}
	return colNames, nil
}

// getSchemaInfo returns the schema metadata needed to build DML queries for the
// given schema.table, served from the internal caches and falling back to
// postgres for whatever is missing.
func (o *pgSchemaObserver) getSchemaInfo(ctx context.Context, schema, table string) (schemaInfo, error) {
	generatedColumns, err := o.getGeneratedColumnNames(ctx, schema, table)
	if err != nil {
		return schemaInfo{}, err
	}

	alwaysIdentityColumns, err := o.getAlwaysIdentityColumnNames(ctx, schema, table)
	if err != nil {
		return schemaInfo{}, err
	}

	sequenceColumns, err := o.getSequenceColumns(ctx, schema, table)
	if err != nil {
		return schemaInfo{}, err
	}

	enumColumns, err := o.getEnumColumnNames(ctx, schema, table)
	if err != nil {
		return schemaInfo{}, err
	}

	return schemaInfo{
		generatedColumns:      generatedColumns,
		alwaysIdentityColumns: alwaysIdentityColumns,
		sequenceColumns:       sequenceColumns,
		enumColumns:           enumColumns,
	}, nil
}

func (o *pgSchemaObserver) update(ddlEvent *wal.DDLEvent) {
	if ddlEvent == nil {
		return
	}

	tableObjects := append(ddlEvent.GetTableObjects(), ddlEvent.GetTableColumnObjects()...)
	o.updateGeneratedColumnNames(tableObjects)
	o.updateColumnSequences(tableObjects)
	o.invalidateEnumColumns(tableObjects)
	mvObjects := ddlEvent.GetMaterializedViewObjects()
	if len(mvObjects) > 0 {
		o.updateMaterializedViews(ddlEvent, mvObjects)
	}
}

// updateGeneratedColumnNames will update the internal cache with the table
// columns for the schema log on input. Identity columns are added to
// generatedColumns via IsGenerated() (preserved historical behavior so live
// INSERTs let the target auto-generate ids and the sequence increments
// naturally). GENERATED ALWAYS AS IDENTITY columns are additionally tracked in
// alwaysIdentityTableColumns so UPDATE SET clauses can drop them even on
// cache paths where generatedColumns is empty (e.g. populated via SQL query).
func (o *pgSchemaObserver) updateGeneratedColumnNames(tables []wal.DDLObject) {
	for _, table := range tables {
		key := pglib.QuoteQualifiedIdentifier(table.Schema, table.GetName())
		generatedColumns := make(map[string]struct{}, len(table.Columns))
		alwaysIdentityColumns := make(map[string]struct{}, len(table.Columns))
		for _, c := range table.Columns {
			if c.IsAlwaysIdentity() {
				alwaysIdentityColumns[pglib.QuoteIdentifier(c.Name)] = struct{}{}
			}
			if c.IsGenerated() {
				generatedColumns[pglib.QuoteIdentifier(c.Name)] = struct{}{}
			}
		}

		o.generatedTableColumns.Set(key, generatedColumns)
		o.alwaysIdentityTableColumns.Set(key, alwaysIdentityColumns)
	}
}

// updateMaterializedViews will update the internal cache with the materialized
// views for the schema log on input.
func (o *pgSchemaObserver) updateMaterializedViews(ddlEvent *wal.DDLEvent, mvs []wal.DDLObject) {
	key := pglib.QuoteIdentifier(ddlEvent.SchemaName)

	existingMVs, found := o.materializedViews.Get(key)
	switch {
	case ddlEvent.IsDropEvent():
		// remove dropped materialized views from the cache
		if !found {
			return
		}
		for _, mv := range mvs {
			delete(existingMVs, pglib.QuoteIdentifier(mv.GetName()))
		}
		o.materializedViews.Set(key, existingMVs)

	default:
		mvNames := make(map[string]struct{}, len(mvs))
		if found {
			mvNames = existingMVs
		}
		for _, mv := range mvs {
			mvNames[pglib.QuoteIdentifier(mv.GetName())] = struct{}{}
		}
		o.materializedViews.Set(key, mvNames)

	}
}

func (o *pgSchemaObserver) updateColumnSequences(tables []wal.DDLObject) {
	for _, table := range tables {
		key := pglib.QuoteQualifiedIdentifier(table.Schema, table.GetName())
		seqColMap := make(map[string]string)
		for _, col := range table.Columns {
			if col.HasSequence() {
				seqColMap[pglib.QuoteIdentifier(col.Name)] = col.GetSequenceName()
			}
		}
		o.columnTableSequences.Set(key, seqColMap)
	}
}

// invalidateEnumColumns drops the cached enum-column set for the affected
// tables so it is re-queried lazily. The DDL event does not carry the type
// category needed to recompute enum membership in-memory, so a DDL that alters
// a column type (to or from an enum) simply evicts the stale entry.
//
// The table name must come from GetTable() rather than GetName(): the input
// mixes table objects ("schema.table") with table column objects
// ("schema.table.column"), and for the latter GetName() returns the column
// name, which would evict a key that never existed and leave the real entry
// stale.
func (o *pgSchemaObserver) invalidateEnumColumns(tables []wal.DDLObject) {
	// bump before deleting so a lookup racing this invalidation cannot cache a
	// pre-DDL answer after its key has been evicted.
	o.enumCacheEpoch.Add(1)
	for _, table := range tables {
		key := pglib.QuoteQualifiedIdentifier(table.Schema, table.GetTable())
		o.enumTableColumns.Delete(key)
	}
}

const generatedTableColumnsQuery = `SELECT attname FROM pg_attribute
		WHERE attnum > 0
		AND attrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2)
		AND attgenerated != ''`

func (o *pgSchemaObserver) queryGeneratedColumnNames(ctx context.Context, schemaName, tableName string) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, schemaQueryTimeout)
	defer cancel()

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

const alwaysIdentityTableColumnsQuery = `SELECT attname FROM pg_attribute
		WHERE attnum > 0
		AND attrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2)
		AND attidentity = 'a'`

func (o *pgSchemaObserver) queryAlwaysIdentityColumnNames(ctx context.Context, schemaName, tableName string) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, schemaQueryTimeout)
	defer cancel()

	columnNames := map[string]struct{}{}
	rows, err := o.pgConn.Query(ctx, alwaysIdentityTableColumnsQuery, tableName, schemaName)
	if err != nil {
		return nil, fmt.Errorf("getting table always-identity column names for table %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scanning table always-identity column name: %w", err)
		}
		columnNames[pglib.QuoteIdentifier(columnName)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columnNames, nil
}

// enumTableColumnsQuery returns the columns of a table whose type resolves to a
// user-defined enum. pgx registers no binary codec for such database-specific
// OIDs, so these columns force text-format COPY, where the value is written as
// the literal postgres text representation and parsed by the target's type
// input function.
//
// Four shapes are covered: a scalar enum, an array of enums, a domain over an
// enum, and a domain over an array of enums. A domain over a domain is not
// resolved (it would need a recursive walk of typbasetype) and still takes the
// binary path.
const enumTableColumnsQuery = `SELECT a.attname
	FROM pg_attribute a
	JOIN pg_class c ON c.oid = a.attrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	JOIN pg_type t ON t.oid = a.atttypid
	LEFT JOIN pg_type elem ON elem.oid = t.typelem
	LEFT JOIN pg_type base ON base.oid = t.typbasetype
	LEFT JOIN pg_type baseelem ON baseelem.oid = base.typelem
	WHERE n.nspname = $1 AND c.relname = $2
	AND a.attnum > 0 AND NOT a.attisdropped
	AND (
		t.typtype = 'e'
		OR (t.typtype = 'b' AND elem.typtype = 'e')
		OR (t.typtype = 'd' AND (base.typtype = 'e' OR baseelem.typtype = 'e'))
	)`

func (o *pgSchemaObserver) queryEnumColumnNames(ctx context.Context, schemaName, tableName string) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, schemaQueryTimeout)
	defer cancel()

	columnNames := map[string]struct{}{}
	rows, err := o.pgConn.Query(ctx, enumTableColumnsQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table enum column names for table %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scanning table enum column name: %w", err)
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
	ctx, cancel := context.WithTimeout(ctx, schemaQueryTimeout)
	defer cancel()

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
	ctx, cancel := context.WithTimeout(ctx, schemaQueryTimeout)
	defer cancel()

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
