// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/progress"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/wal/processor"
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
	adapter      *adapter
	processor    processor.Processor
	tableWorkers uint
	batchBytes   uint64

	// progress tracking, shared with the snapshot generator.
	progressTracking bool
	progressBars     *synclib.Map[string, progress.Bar]
}

// beginSchema opens the transaction that exports the shared transaction
// snapshot and keeps it open for the duration of fn, so that every readTable
// call can import it. The snapshot is only exported when the schema has at least
// one ctid table to read.
func (r *ctidReader) beginSchema(ctx context.Context, st *schemaTables, fn func(context.Context, *readSession) error) error {
	// use a transaction snapshot to ensure the table rows can be parallelised.
	// The transaction snapshot is available for use only until the end of the
	// transaction that exported it.
	// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION
	return r.conn.ExecInTxWithOptions(ctx, func(tx pglib.Tx) error {
		session := &readSession{}
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

func (r *ctidReader) readTable(ctx context.Context, session *readSession, table *table) error {
	snapshotID := session.snapshotID
	tableInfo, err := r.getTableInfo(ctx, table.schema, table.name, snapshotID)
	if err != nil {
		return err
	}
	if tableInfo.isEmpty() {
		return nil
	}
	table.rowSize = tableInfo.avgRowBytes

	// If one page range fails, we abort the entire table snapshot. The
	// snapshot relies on the transaction snapshot id to ensure all workers
	// have the same table view, which allows us to use the ctid to
	// parallelise the work.
	rangeChan := make(chan pageRange, tableInfo.pageCount)
	errGroup, ctx := errgroup.WithContext(ctx)
	for i := uint(0); i < r.tableWorkers; i++ {
		errGroup.Go(func() error {
			return r.snapshotTableRangeWorker(ctx, snapshotID, table, rangeChan)
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

func (r *ctidReader) snapshotTableRangeWorker(ctx context.Context, snapshotID string, table *table, pageRangeChan <-chan pageRange) error {
	for pageRange := range pageRangeChan {
		if err := r.snapshotTableRange(ctx, snapshotID, table, pageRange); err != nil {
			return err
		}
	}
	return nil
}

var pageRangeQuery = "SELECT * FROM ONLY %s WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'"

func (r *ctidReader) snapshotTableRange(ctx context.Context, snapshotID string, table *table, pageRange pageRange) error {
	return execInSnapshotTx(ctx, r.conn, snapshotID, func(tx pglib.Tx) error {
		r.logger.Debug(fmt.Sprintf("querying table page range %d-%d", pageRange.start, pageRange.end), loglib.Fields{
			"schema": table.schema, "table": table.name, "snapshotID": snapshotID,
		})

		query := fmt.Sprintf(pageRangeQuery, pglib.QuoteQualifiedIdentifier(table.schema, table.name), pageRange.start, pageRange.end)
		rows, err := tx.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("querying table rows: %w", err)
		}
		defer rows.Close()

		// resolve the column metadata (names/types) and timestamp once per page
		// range, since the field descriptions are identical for every row in the
		// result set.
		rowAdapter := r.adapter.newRowEventAdapter(ctx, table.schema, table.name, rows.FieldDescriptions())
		rowCount := uint(0)
		for rows.Next() {
			rowCount++
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				values, err := rows.Values()
				if err != nil {
					return fmt.Errorf("retrieving rows values: %w", err)
				}

				event := rowAdapter.rowToWalEvent(values)
				if event == nil {
					continue
				}

				if err := r.processor.ProcessWALEvent(ctx, event); err != nil {
					return fmt.Errorf("processing snapshot row: %w", err)
				}
			}
		}

		if r.progressTracking {
			bar, found := r.progressBars.Get(table.schema)
			if found {
				bar.Add64(int64(rowCount) * table.rowSize)
			}
		}

		r.logger.Debug(fmt.Sprintf("%d rows processed", rowCount), loglib.Fields{
			"schema": table.schema, "table": table.name, "snapshotID": snapshotID,
		})

		return rows.Err()
	})
}

const (
	// use pg_table_size instead of pg_total_relation_size since we only care about the size of the table itself and toast tables, not indices.
	// pg_relation_size will return only the size of the table itself, without toast tables.
	tableInfoQuery = `SELECT
  (pg_table_size(c.oid) / COALESCE(NULLIF(c.relpages, 0),1)) AS avg_page_size_bytes,
  CASE
	WHEN c.reltuples > 0 THEN
		ROUND(pg_table_size(c.oid) / c.reltuples)
	ELSE
		0
  END AS avg_row_size
FROM
  pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
  c.relname = $1
  AND n.nspname = $2
  AND c.relkind = 'r';`

	// select the max page for the relation instead of using pg_class.relpages, it may not contain an accurate value if
	// the table is small, the table has active inserts, or the database has not been vacuumed/analyzed recently.
	maxPageQuery = `SELECT MAX(ctid) FROM ONLY %s;`
)

func (r *ctidReader) getTableInfo(ctx context.Context, schemaName, tableName, snapshotID string) (*tableInfo, error) {
	tableInfo := &tableInfo{}
	err := execInSnapshotTx(ctx, r.conn, snapshotID, func(tx pglib.Tx) error {
		// make sure the schema and table names are unquoted since the system
		// catalogs store unquoted names
		err := tx.QueryRow(ctx,
			[]any{&tableInfo.avgPageBytes, &tableInfo.avgRowBytes},
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

		tableInfo.calculateBatchPageSize(r.batchBytes)

		r.logger.Debug(fmt.Sprintf("table page count: %d, batch page size: %d", tableInfo.pageCount, tableInfo.batchPageSize), loglib.Fields{
			"schema": schemaName, "table": tableName, "snapshotID": snapshotID,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tableInfo, nil
}
