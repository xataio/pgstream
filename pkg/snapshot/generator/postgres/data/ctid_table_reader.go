// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	"golang.org/x/sync/errgroup"
)

// ctidReader reads a schema's tables by ranging over a stable transaction
// snapshot using the ctid. The transaction snapshot is exported once per schema
// in beginSchema and imported by every page range transaction, which lets the
// reader parallelise the work across page ranges while keeping a consistent
// view of each table.
type ctidReader struct {
	conn         pglib.Querier
	logger       loglib.Logger
	sink         rowSink
	tableWorkers uint
	batchBytes   uint64
}

// beginSchema opens the transaction that exports the shared transaction
// snapshot and keeps it open for the duration of fn, so that every read the
// session performs can import it. The snapshot is only exported when the schema
// has at least one ctid table to read.
func (r *ctidReader) beginSchema(ctx context.Context, st *schemaTables, fn func(context.Context, readSession) error) error {
	// use a transaction snapshot to ensure the table rows can be parallelised.
	// The transaction snapshot is available for use only until the end of the
	// transaction that exported it.
	// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION
	return r.conn.ExecInTxWithOptions(ctx, func(tx pglib.Tx) error {
		session := &ctidSession{
			reader:       r,
			schemaTables: st,
		}
		if len(st.tables) > 0 {
			snapshotID, err := exportSnapshot(ctx, tx)
			if err != nil {
				return snapshot.NewSchemaErrors(st.schema, err)
			}
			session.snapshotID = snapshotID
		}

		return fn(ctx, session)
	}, snapshotTxOptions())
}

// ctidSession reads the tables of a single schema over the transaction snapshot
// exported by beginSchema. Every transaction it opens imports that snapshot, so
// all the workers observe the same view of the schema, which is what makes it
// safe to split a table into page ranges read in parallel.
type ctidSession struct {
	reader       *ctidReader
	schemaTables *schemaTables
	// snapshotID is empty when the schema has no tables to read, in which case
	// no read is performed: an empty id would make SET TRANSACTION SNAPSHOT
	// fail.
	snapshotID string
}

func (s *ctidSession) logFields() loglib.Fields {
	return loglib.Fields{"snapshotID": s.snapshotID}
}

// execInTx runs fn in a transaction that observes the same view of the database
// as the transaction that exported this session's snapshot.
func (s *ctidSession) execInTx(ctx context.Context, fn func(tx pglib.Tx) error) error {
	return execInSnapshotTx(ctx, s.reader.conn, s.snapshotID, fn)
}

func (s *ctidSession) readTable(ctx context.Context, table *table) error {
	tableInfo, err := s.getTableInfo(ctx, table.schema, table.name)
	if err != nil {
		return err
	}
	if tableInfo.isEmpty() {
		return nil
	}
	table.rowSize = tableInfo.avgRowBytes

	// an empty intersection must not fall back to SELECT *
	columns, pinLost := readableColumns(table.columns, tableInfo.columns)
	if pinLost {
		return fmt.Errorf("%w: no captured column of %s.%s exists on the source",
			ErrSchemaChangedDuringSnapshot,
			pglib.UnquoteIdentifier(table.schema), pglib.UnquoteIdentifier(table.name))
	}
	table.columns = columns

	// If one page range fails, we abort the entire table snapshot. The
	// snapshot relies on the transaction snapshot id to ensure all workers
	// have the same table view, which allows us to use the ctid to
	// parallelise the work.
	rangeChan := make(chan pageRange, tableInfo.pageCount)
	errGroup, ctx := errgroup.WithContext(ctx)
	for i := uint(0); i < s.reader.tableWorkers; i++ {
		errGroup.Go(func() error {
			return s.snapshotTableRangeWorker(ctx, table, rangeChan)
		})
	}

	// page count returned by postgres starts at 0, so we need to include it
	// when creating the page ranges.
	for start := uint(0); start <= uint(tableInfo.pageCount); start += tableInfo.batchPageSize {
		rangeChan <- pageRange{
			start: start,
			end:   start + tableInfo.batchPageSize,
		}
	}

	// wait for all table ranges to complete
	close(rangeChan)
	return errGroup.Wait()
}

func (s *ctidSession) snapshotTableRangeWorker(ctx context.Context, table *table, pageRangeChan <-chan pageRange) error {
	for pageRange := range pageRangeChan {
		if err := s.snapshotTableRange(ctx, table, pageRange); err != nil {
			return err
		}
	}
	return nil
}

const pageRangeQuery = "SELECT %s FROM ONLY %s WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'"

// buildPageRangeQuery spells columns out.
func buildPageRangeQuery(t *table, r pageRange) string {
	quotedTable := pglib.QuoteQualifiedIdentifier(t.schema, t.name)
	if len(t.columns) == 0 {
		return fmt.Sprintf(pageRangeQuery, allColumns, quotedTable, r.start, r.end)
	}

	quotedColumns := make([]string, len(t.columns))
	for i, column := range t.columns {
		quotedColumns[i] = pglib.QuoteRawIdentifier(column)
	}
	return fmt.Sprintf(pageRangeQuery, strings.Join(quotedColumns, ", "), quotedTable, r.start, r.end)
}

func (s *ctidSession) snapshotTableRange(ctx context.Context, table *table, pageRange pageRange) error {
	return s.execInTx(ctx, func(tx pglib.Tx) error {
		s.reader.logger.Debug(fmt.Sprintf("querying table page range %d-%d", pageRange.start, pageRange.end), loglib.Fields{
			"schema": table.schema, "table": table.name, "snapshotID": s.snapshotID,
		})

		query := buildPageRangeQuery(table, pageRange)
		rows, err := tx.Query(ctx, query)
		if err != nil {
			// something this query names vanished
			var relationErr *pglib.ErrRelationDoesNotExist
			if errors.As(err, &relationErr) {
				return fmt.Errorf("%w: querying table rows: %w", ErrSchemaChangedDuringSnapshot, err)
			}
			return fmt.Errorf("querying table rows: %w", err)
		}
		defer rows.Close()

		rowCount, err := s.reader.sink.emit(ctx, table, rows)
		if err != nil {
			return err
		}

		s.reader.logger.Debug(fmt.Sprintf("%d rows processed", rowCount), loglib.Fields{
			"schema": table.schema, "table": table.name, "snapshotID": s.snapshotID,
		})

		return nil
	})
}

// tableInfoQuery shares the capture rule.
var tableInfoQuery = fmt.Sprintf(tableInfoQueryFmt, pglib.SelectStarColumnPredicate)

const (
	// use pg_table_size instead of pg_total_relation_size since we only care about the size of the table itself and toast tables, not indices.
	// pg_relation_size will return only the size of the table itself, without toast tables.
	tableInfoQueryFmt = `SELECT
  (pg_table_size(c.oid) / COALESCE(NULLIF(c.relpages, 0),1)) AS avg_page_size_bytes,
  CASE
	WHEN c.reltuples > 0 THEN
		ROUND(pg_table_size(c.oid) / c.reltuples)
	ELSE
		0
  END AS avg_row_size,
  ARRAY(
    SELECT a.attname::text FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = c.oid AND %s
    ORDER BY a.attnum
  ) AS columns
FROM
  pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE
  c.relname = $1
  AND n.nspname = $2
  AND c.relkind = 'r';`

	// select the max page for the relation instead of using pg_class.relpages, it may not contain an accurate value if
	// the table is small, the table has active inserts, or the database has not been vacuumed/analyzed recently.
	maxPageQuery = `SELECT MAX(ctid) FROM ONLY %s;`

	tablesBytesQuery = `SELECT SUM(pg_table_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = ANY($2) AND c.relkind = 'r';`
)

func (s *ctidSession) getTableInfo(ctx context.Context, schemaName, tableName string) (*tableInfo, error) {
	tableInfo := &tableInfo{}
	err := s.execInTx(ctx, func(tx pglib.Tx) error {
		// make sure the schema and table names are unquoted since the system
		// catalogs store unquoted names
		err := tx.QueryRow(ctx,
			[]any{&tableInfo.avgPageBytes, &tableInfo.avgRowBytes, &tableInfo.columns},
			tableInfoQuery,
			pglib.UnquoteIdentifier(tableName),
			pglib.UnquoteIdentifier(schemaName))
		if err != nil {
			return fmt.Errorf("getting page information for table %s.%s: %w", schemaName, tableName, err)
		}

		var ctid pgtype.TID
		if err := tx.QueryRow(ctx, []any{&ctid}, fmt.Sprintf(maxPageQuery, pglib.QuoteQualifiedIdentifier(schemaName, tableName))); err != nil {
			return fmt.Errorf("getting max page for table %s.%s: %w", schemaName, tableName, err)
		}
		tableInfo.pageCount = int(ctid.BlockNumber)

		tableInfo.calculateBatchPageSize(s.reader.batchBytes)

		s.reader.logger.Debug(fmt.Sprintf("table page count: %d, batch page size: %d", tableInfo.pageCount, tableInfo.batchPageSize), loglib.Fields{
			"schema": schemaName, "table": tableName, "snapshotID": s.snapshotID,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tableInfo, nil
}

// totalBytes returns the on disk size of the session's schema tables, as seen
// by the session's transaction snapshot.
func (s *ctidSession) totalBytes(ctx context.Context) (int64, error) {
	schema, tables := s.schemaTables.schema, s.schemaTables.tables

	totalBytes := int64(0)
	s.reader.logger.Debug("querying total bytes for schema", loglib.Fields{
		"schema": schema, "tables": tables, "snapshotID": s.snapshotID,
	})

	// make sure the schema and table names are unquoted since the system
	// catalogs store unquoted names
	unquotedTables := make([]string, len(tables))
	for i, table := range tables {
		unquotedTables[i] = pglib.UnquoteIdentifier(table)
	}

	err := s.execInTx(ctx, func(tx pglib.Tx) error {
		err := tx.QueryRow(ctx, []any{&totalBytes}, tablesBytesQuery, pglib.UnquoteIdentifier(schema), unquotedTables)
		if err != nil {
			return fmt.Errorf("retrieving total bytes for schema: %w", err)
		}
		return nil
	})

	return totalBytes, err
}
