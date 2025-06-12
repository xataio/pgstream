// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/snapshot"
	"golang.org/x/sync/errgroup"
)

type SnapshotGenerator struct {
	logger loglib.Logger
	conn   pglib.Querier
	mapper mapper

	// workers per snapshot, parallelise the snapshot creation for each schema
	snapshotWorkers uint
	// workers per schema, parallelise the snapshot creation for each table
	schemaWorkers uint
	// workers per table, parallelise the snapshot creation for each page range
	tableWorkers uint
	batchBytes   uint64

	// Function called for processing produced rows.
	rowsProcessor          snapshot.RowsProcessor
	tableSnapshotGenerator snapshotTableFn
}

type mapper interface {
	TypeForOID(context.Context, uint32) (string, error)
}

type tablePageInfo struct {
	pageCount    int
	avgPageBytes int64
}

type pageRange struct {
	start uint
	end   uint
}

type schemaTables struct {
	schema string
	tables []string
}

type snapshotTableFn func(ctx context.Context, snapshotID string, schema, table string) error

type Option func(sg *SnapshotGenerator)

func NewSnapshotGenerator(ctx context.Context, cfg *Config, rowsProcessor snapshot.RowsProcessor, opts ...Option) (*SnapshotGenerator, error) {
	conn, err := pglib.NewConnPool(ctx, cfg.URL)
	if err != nil {
		return nil, err
	}

	sg := &SnapshotGenerator{
		logger:          loglib.NewNoopLogger(),
		mapper:          pglib.NewMapper(conn),
		conn:            conn,
		rowsProcessor:   rowsProcessor,
		batchBytes:      cfg.batchBytes(),
		tableWorkers:    cfg.tableWorkers(),
		schemaWorkers:   cfg.schemaWorkers(),
		snapshotWorkers: cfg.snapshotWorkers(),
	}

	sg.tableSnapshotGenerator = sg.snapshotTable

	for _, opt := range opts {
		opt(sg)
	}

	return sg, nil
}

func WithLogger(logger loglib.Logger) Option {
	return func(sg *SnapshotGenerator) {
		sg.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_data_snapshot_generator",
		})
	}
}

func WithInstrumentation(i *otel.Instrumentation) Option {
	return func(sg *SnapshotGenerator) {
		var err error
		sg.conn, err = pglibinstrumentation.NewQuerier(sg.conn, i)
		if err != nil {
			// this should never happen
			panic(err)
		}

		ig := newInstrumentedTableSnapshotGenerator(sg.tableSnapshotGenerator, i)
		sg.tableSnapshotGenerator = ig.snapshotTable
	}
}

func (sg *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) (err error) {
	defer func() {
		// make sure we close the rows processor once the snapshot is completed.
		// It will wait until all rows are processed before returning.
		sg.rowsProcessor.Close()
	}()

	// parallelise the snapshot creation for each schema as configured by the snapshot workers.
	errGroup, ctx := errgroup.WithContext(ctx)
	schemaTablesChan := make(chan *schemaTables)
	schemaErrs := make(map[string]error, len(ss.SchemaTables))
	for i := uint(0); i < sg.snapshotWorkers; i++ {
		errGroup.Go(func() error {
			for schemaTables := range schemaTablesChan {
				sg.logger.Info("creating snapshot for schema", loglib.Fields{"schema": schemaTables.schema, "tables": schemaTables.tables})
				if err := sg.createSchemaSnapshot(ctx, schemaTables); err != nil {
					sg.logger.Error(err, "creating snapshot for schema", loglib.Fields{"schema": schemaTables.schema, "tables": schemaTables.tables, "error": err.Error()})
					schemaErrs[schemaTables.schema] = err
				}
			}
			return nil
		})
	}
	for schema, tables := range ss.SchemaTables {
		if len(tables) == 0 {
			sg.logger.Debug("skipping empty schema", loglib.Fields{"schema": schema})
			continue
		}
		schemaTablesChan <- &schemaTables{
			schema: schema,
			tables: tables,
		}
	}
	close(schemaTablesChan)

	if err := errGroup.Wait(); err != nil {
		return err
	}

	// collect all schema errors and return them as a single error
	return sg.collectSchemaErrors(schemaErrs)
}

func (sg *SnapshotGenerator) Close() error {
	return sg.conn.Close(context.Background())
}

func (sg *SnapshotGenerator) createSchemaSnapshot(ctx context.Context, schemaTables *schemaTables) error {
	// use a transaction snapshot to ensure the table rows can be parallelised.
	// The transaction snapshot is available for use only until the end of the
	// transaction that exported it.
	// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION
	return sg.conn.ExecInTxWithOptions(ctx, func(tx pglib.Tx) error {
		snapshotID, err := sg.exportSnapshot(ctx, tx)
		if err != nil {
			return snapshot.NewSchemaErrors(schemaTables.schema, err)
		}

		tableChan := make(chan string, len(schemaTables.tables))
		// a map of table errors per worker to avoid race conditions
		workerTableErrs := make([]map[string]error, sg.schemaWorkers)
		wg := &sync.WaitGroup{}
		// start as many go routines as configured concurrent workers per schema
		for i := uint(0); i < sg.schemaWorkers; i++ {
			wg.Add(1)
			workerTableErrs[i] = make(map[string]error, len(schemaTables.tables))
			go sg.createSnapshotWorker(ctx, wg, schemaTables.schema, snapshotID, tableChan, workerTableErrs[i])
		}

		for _, table := range schemaTables.tables {
			tableChan <- table
		}

		close(tableChan)
		wg.Wait()

		return sg.collectTableErrors(schemaTables.schema, workerTableErrs)
	}, snapshotTxOptions())
}

func (sg *SnapshotGenerator) createSnapshotWorker(ctx context.Context, wg *sync.WaitGroup, schema, snapshotID string, tableChan <-chan string, tableErrMap map[string]error) {
	defer wg.Done()
	for table := range tableChan {
		logFields := loglib.Fields{"schema": schema, "table": table, "snapshotID": snapshotID}
		sg.logger.Info("snapshotting table", logFields)

		if err := sg.tableSnapshotGenerator(ctx, snapshotID, schema, table); err != nil {
			sg.logger.Error(err, "snapshotting table", logFields)
			// errors will get notified unless the table doesn't exist
			if !errors.Is(err, pglib.ErrNoRows) {
				tableErrMap[table] = err
			}
		}
		sg.logger.Info("table snapshot completed", logFields)
	}
}

func (sg *SnapshotGenerator) collectTableErrors(schema string, workerTableErrs []map[string]error) error {
	var schemaErrs *snapshot.SchemaErrors
	for _, worker := range workerTableErrs {
		for table, err := range worker {
			if err == nil {
				continue
			}
			if schemaErrs == nil {
				schemaErrs = &snapshot.SchemaErrors{
					Schema: schema,
				}
			}
			schemaErrs.AddTableError(table, err)
		}
	}
	if schemaErrs != nil {
		return schemaErrs
	}

	return nil
}

func (sg *SnapshotGenerator) collectSchemaErrors(workerSchemaErrs map[string]error) error {
	snapshotErrs := make(snapshot.Errors, len(workerSchemaErrs))
	for schema, err := range workerSchemaErrs {
		if err == nil {
			continue
		}
		snapshotErrs.AddError(schema, snapshot.NewSchemaErrors(schema, err))
	}
	if len(snapshotErrs) > 0 {
		return snapshotErrs
	}

	return nil
}

func (sg *SnapshotGenerator) snapshotTable(ctx context.Context, snapshotID string, schema, table string) error {
	tablePageInfo, err := sg.getTablePageInfo(ctx, schema, table, snapshotID)
	if err != nil {
		return err
	}
	// the table is empty
	if tablePageInfo.pageCount < 0 {
		return nil
	}

	// If one page range fails, we abort the entire table snapshot. The
	// snapshot relies on the transaction snapshot id to ensure all workers
	// have the same table view, which allows us to use the ctid to
	// parallelise the work.
	rangeChan := make(chan pageRange, tablePageInfo.pageCount)
	errGroup, ctx := errgroup.WithContext(ctx)
	for i := uint(0); i < sg.tableWorkers; i++ {
		errGroup.Go(func() error {
			return sg.snapshotTableRangeWorker(ctx, snapshotID, schema, table, rangeChan)
		})
	}

	// automatically determine the batch page size based on the average page
	// size and the configured batch bytes limit.
	batchPageSize := tablePageInfo.getBatchPageSize(sg.batchBytes)
	sg.logger.Debug("batch page size for table", loglib.Fields{
		"schema": schema, "table": table, "batch_page_size": batchPageSize,
		"page_count": tablePageInfo.pageCount, "avg_page_bytes": tablePageInfo.avgPageBytes,
	})
	// page count returned by postgres starts at 0, so we need to include it
	// when creating the page ranges.
	for start := uint(0); start <= uint(tablePageInfo.pageCount); start += batchPageSize {
		rangeChan <- pageRange{
			start: start,
			end:   start + batchPageSize,
		}
	}

	// wait for all table ranges to complete
	close(rangeChan)
	return errGroup.Wait()
}

func (sg *SnapshotGenerator) snapshotTableRangeWorker(ctx context.Context, snapshotID, schema, table string, pageRangeChan <-chan (pageRange)) error {
	for pageRange := range pageRangeChan {
		if err := sg.snapshotTableRange(ctx, snapshotID, schema, table, pageRange); err != nil {
			return err
		}
	}
	return nil
}

var pageRangeQuery = "SELECT * FROM %s WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'"

func (sg *SnapshotGenerator) snapshotTableRange(ctx context.Context, snapshotID, schema, table string, pageRange pageRange) error {
	return sg.execInSnapshotTx(ctx, snapshotID, func(tx pglib.Tx) error {
		sg.logger.Debug(fmt.Sprintf("querying table page range %d-%d", pageRange.start, pageRange.end), loglib.Fields{
			"schema": schema, "table": table, "snapshotID": snapshotID,
		})

		query := fmt.Sprintf(pageRangeQuery, pglib.QuoteQualifiedIdentifier(schema, table), pageRange.start, pageRange.end)
		rows, err := tx.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("querying table rows: %w", err)
		}
		defer rows.Close()

		fieldDescriptions := rows.FieldDescriptions()
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

				columns := sg.toSnapshotColumns(ctx, fieldDescriptions, values)
				if len(columns) == 0 {
					continue
				}

				if err := sg.rowsProcessor.ProcessRow(ctx, &snapshot.Row{
					Schema:  schema,
					Table:   table,
					Columns: columns,
				}); err != nil {
					return fmt.Errorf("processing snapshot row: %w", err)
				}
			}
		}

		sg.logger.Debug(fmt.Sprintf("%d rows processed", rowCount))

		return rows.Err()
	})
}

func (sg *SnapshotGenerator) toSnapshotColumns(ctx context.Context, fieldDescriptions []pgconn.FieldDescription, values []any) []snapshot.Column {
	columns := make([]snapshot.Column, 0, len(fieldDescriptions))
	for i, value := range values {
		dataType, err := sg.mapper.TypeForOID(ctx, fieldDescriptions[i].DataTypeOID)
		if err != nil {
			sg.logger.Warn(err, "unknown data type OID", loglib.Fields{"data_type_oid": fieldDescriptions[i].DataTypeOID})
			continue
		}

		columns = append(columns, snapshot.Column{
			Name:  fieldDescriptions[i].Name,
			Type:  dataType,
			Value: value,
		})
	}

	return columns
}

// use pg_table_size instead of pg_total_relation_size since we only care about the size of the table itself and toast tables, not indices.
// pg_relation_size will return only the size of the table itself, without toast tables.
const tablePageInfoQuery = `SELECT
  c.relpages AS page_count,
  (pg_table_size(c.oid) / COALESCE(NULLIF(c.relpages, 0),1)) AS avg_page_size_bytes
FROM
  pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
  c.relname = $1
  AND n.nspname = $2
  AND c.relkind = 'r';`

func (sg *SnapshotGenerator) getTablePageInfo(ctx context.Context, schemaName, tableName, snapshotID string) (*tablePageInfo, error) {
	tablePageInfo := &tablePageInfo{}
	err := sg.execInSnapshotTx(ctx, snapshotID, func(tx pglib.Tx) error {
		if err := tx.QueryRow(ctx, tablePageInfoQuery, tableName, schemaName).Scan(&tablePageInfo.pageCount, &tablePageInfo.avgPageBytes); err != nil {
			return fmt.Errorf("getting page information for table %s.%s: %w", schemaName, tableName, err)
		}

		sg.logger.Debug(fmt.Sprintf("table page count: %d, page avg bytes: %d", tablePageInfo.pageCount, tablePageInfo.avgPageBytes), loglib.Fields{
			"schema": schemaName, "table": tableName, "snapshotID": snapshotID,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tablePageInfo, nil
}

const exportSnapshotQuery = `SELECT pg_export_snapshot()`

func (sg *SnapshotGenerator) exportSnapshot(ctx context.Context, tx pglib.Tx) (string, error) {
	var snapshotID string
	if err := tx.QueryRow(ctx, exportSnapshotQuery).Scan(&snapshotID); err != nil {
		return "", fmt.Errorf("exporting snapshot: %w", err)
	}
	return snapshotID, nil
}

func (sg *SnapshotGenerator) setTransactionSnapshot(ctx context.Context, tx pglib.Tx, snapshotID string) error {
	_, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID))
	if err != nil {
		return fmt.Errorf("setting transaction snapshot: %w", err)
	}
	return nil
}

func (sg *SnapshotGenerator) execInSnapshotTx(ctx context.Context, snapshotID string, fn func(tx pglib.Tx) error) error {
	return sg.conn.ExecInTxWithOptions(ctx, func(tx pglib.Tx) error {
		if err := sg.setTransactionSnapshot(ctx, tx, snapshotID); err != nil {
			return err
		}

		return fn(tx)
	}, snapshotTxOptions())
}

func snapshotTxOptions() pglib.TxOptions {
	return pglib.TxOptions{
		IsolationLevel: pglib.RepeatableRead,
		AccessMode:     pglib.ReadOnly,
	}
}

func (t *tablePageInfo) getBatchPageSize(bytes uint64) uint {
	// at least one page is needed to process the table
	if t.pageCount == 0 {
		return 1
	}

	// no limit on bytes, return all pages
	if bytes == 0 || t.avgPageBytes == 0 {
		return uint(t.pageCount)
	}

	batchPageSize := bytes / uint64(t.avgPageBytes)
	// at least one page is needed to process the table
	if batchPageSize == 0 {
		batchPageSize = 1
	}

	// don't exceed the total page count
	if batchPageSize > uint64(t.pageCount) {
		batchPageSize = uint64(t.pageCount)
	}

	return uint(batchPageSize)
}
