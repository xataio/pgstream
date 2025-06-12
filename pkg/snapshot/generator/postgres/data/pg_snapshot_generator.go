// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	"github.com/xataio/pgstream/internal/progress"
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

	progressTracking   bool
	progressBars       map[string]progress.Bar
	progressBarBuilder func(totalBytes int64, description string) progress.Bar
}

type mapper interface {
	TypeForOID(context.Context, uint32) (string, error)
}

type tableInfo struct {
	pageCount    int
	avgPageBytes int64
	avgRowBytes  int64
}

type pageRange struct {
	start uint
	end   uint
}

type schemaTables struct {
	schema string
	tables []string
}

type table struct {
	schema  string
	name    string
	rowSize int64
}

type snapshotTableFn func(ctx context.Context, snapshotID string, table *table) error

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

func WithProgressTracking() Option {
	return func(sg *SnapshotGenerator) {
		sg.progressTracking = true
		sg.progressBars = make(map[string]progress.Bar)
		sg.progressBarBuilder = func(totalBytes int64, description string) progress.Bar {
			return progress.NewBytesBar(totalBytes, description)
		}
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
				sg.logger.Info("creating data snapshot", loglib.Fields{"schema": schemaTables.schema, "tables": schemaTables.tables})
				if err := sg.createSchemaSnapshot(ctx, schemaTables); err != nil {
					sg.logger.Error(err, "creating data snapshot", loglib.Fields{"schema": schemaTables.schema, "tables": schemaTables.tables, "error": err.Error()})
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
	return sg.conn.ExecInTxWithOptions(ctx, func(tx pglib.Tx) (err error) {
		snapshotID, err := sg.exportSnapshot(ctx, tx)
		if err != nil {
			return snapshot.NewSchemaErrors(schemaTables.schema, err)
		}

		if sg.progressTracking {
			if err := sg.addProgressBar(ctx, snapshotID, schemaTables); err != nil {
				return err
			}
			defer func() {
				if err == nil {
					sg.markProgressBarCompleted(schemaTables.schema)
				}
			}()
		}

		tableChan := make(chan *table, len(schemaTables.tables))
		// a map of table errors per worker to avoid race conditions
		workerTableErrs := make([]map[string]error, sg.schemaWorkers)
		wg := &sync.WaitGroup{}
		// start as many go routines as configured concurrent workers per schema
		for i := uint(0); i < sg.schemaWorkers; i++ {
			wg.Add(1)
			workerTableErrs[i] = make(map[string]error, len(schemaTables.tables))
			go sg.createSnapshotWorker(ctx, wg, snapshotID, tableChan, workerTableErrs[i])
		}

		for _, tableName := range schemaTables.tables {
			tableChan <- &table{
				schema: schemaTables.schema,
				name:   tableName,
			}
		}

		close(tableChan)
		wg.Wait()

		return sg.collectTableErrors(schemaTables.schema, workerTableErrs)
	}, snapshotTxOptions())
}

func (sg *SnapshotGenerator) createSnapshotWorker(ctx context.Context, wg *sync.WaitGroup, snapshotID string, tableChan <-chan *table, tableErrMap map[string]error) {
	defer wg.Done()
	for t := range tableChan {
		logFields := loglib.Fields{"schema": t.schema, "table": t.name, "snapshotID": snapshotID}
		sg.logger.Debug("snapshotting table", logFields)

		if err := sg.tableSnapshotGenerator(ctx, snapshotID, t); err != nil {
			sg.logger.Error(err, "snapshotting table", logFields)
			// errors will get notified unless the table doesn't exist
			if !errors.Is(err, pglib.ErrNoRows) {
				tableErrMap[t.name] = err
			}
		}
		sg.logger.Debug("table snapshot completed", logFields)
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

func (sg *SnapshotGenerator) snapshotTable(ctx context.Context, snapshotID string, table *table) error {
	tableInfo, err := sg.getTableInfo(ctx, table.schema, table.name, snapshotID)
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
	for i := uint(0); i < sg.tableWorkers; i++ {
		errGroup.Go(func() error {
			return sg.snapshotTableRangeWorker(ctx, snapshotID, table, rangeChan)
		})
	}

	// automatically determine the batch page size based on the average page
	// size and the configured batch bytes limit.
	batchPageSize := tableInfo.getBatchPageSize(sg.batchBytes)
	sg.logger.Debug("batch page size for table", loglib.Fields{
		"schema": table.schema, "table": table.name, "batch_page_size": batchPageSize,
		"page_count": tableInfo.pageCount, "avg_page_bytes": tableInfo.avgPageBytes,
	})
	// page count returned by postgres starts at 0, so we need to include it
	// when creating the page ranges.
	for start := uint(0); start <= uint(tableInfo.pageCount); start += batchPageSize {
		rangeChan <- pageRange{
			start: start,
			end:   start + batchPageSize,
		}
	}

	// wait for all table ranges to complete
	close(rangeChan)
	return errGroup.Wait()
}

func (sg *SnapshotGenerator) snapshotTableRangeWorker(ctx context.Context, snapshotID string, table *table, pageRangeChan <-chan (pageRange)) error {
	for pageRange := range pageRangeChan {
		if err := sg.snapshotTableRange(ctx, snapshotID, table, pageRange); err != nil {
			return err
		}
	}
	return nil
}

var pageRangeQuery = "SELECT * FROM %s WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'"

func (sg *SnapshotGenerator) snapshotTableRange(ctx context.Context, snapshotID string, table *table, pageRange pageRange) error {
	return sg.execInSnapshotTx(ctx, snapshotID, func(tx pglib.Tx) error {
		sg.logger.Debug(fmt.Sprintf("querying table page range %d-%d", pageRange.start, pageRange.end), loglib.Fields{
			"schema": table.schema, "table": table.name, "snapshotID": snapshotID,
		})

		query := fmt.Sprintf(pageRangeQuery, pglib.QuoteQualifiedIdentifier(table.schema, table.name), pageRange.start, pageRange.end)
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
					Schema:  table.schema,
					Table:   table.name,
					Columns: columns,
				}); err != nil {
					return fmt.Errorf("processing snapshot row: %w", err)
				}
			}
		}

		if sg.progressTracking {
			sg.logger.Debug(fmt.Sprintf("adding %d rows to progress bar", rowCount))
			sg.progressBars[table.schema].Add64(int64(rowCount) * table.rowSize)
		}

		sg.logger.Debug(fmt.Sprintf("%d rows processed", rowCount), loglib.Fields{
			"schema": table.schema, "table": table.name, "snapshotID": snapshotID,
		})

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

func (sg *SnapshotGenerator) addProgressBar(ctx context.Context, snapshotID string, schemaTables *schemaTables) error {
	totalBytes, err := sg.getSnapshotSchemaTotalBytes(ctx, snapshotID, schemaTables.schema, schemaTables.tables)
	if err != nil {
		return err
	}

	sg.progressBars[schemaTables.schema] = sg.progressBarBuilder(totalBytes, fmt.Sprintf("[cyan][%s][reset] Snapshotting data...", schemaTables.schema))
	return nil
}

func (sg *SnapshotGenerator) markProgressBarCompleted(schema string) {
	bar, found := sg.progressBars[schema]
	if !found {
		return
	}

	bar.Close()
	delete(sg.progressBars, schema)
}

// use pg_table_size instead of pg_total_relation_size since we only care about the size of the table itself and toast tables, not indices.
// pg_relation_size will return only the size of the table itself, without toast tables.
const tableInfoQuery = `SELECT
  c.relpages AS page_count,
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

func (sg *SnapshotGenerator) getTableInfo(ctx context.Context, schemaName, tableName, snapshotID string) (*tableInfo, error) {
	tableInfo := &tableInfo{}
	err := sg.execInSnapshotTx(ctx, snapshotID, func(tx pglib.Tx) error {
		if err := tx.QueryRow(ctx, tableInfoQuery, tableName, schemaName).Scan(&tableInfo.pageCount, &tableInfo.avgPageBytes, &tableInfo.avgRowBytes); err != nil {
			return fmt.Errorf("getting page information for table %s.%s: %w", schemaName, tableName, err)
		}

		sg.logger.Debug(fmt.Sprintf("table page count: %d, page avg bytes: %d", tableInfo.pageCount, tableInfo.avgPageBytes), loglib.Fields{
			"schema": schemaName, "table": tableName, "snapshotID": snapshotID,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tableInfo, nil
}

const tablesBytesQuery = `SELECT SUM(pg_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '%s' AND c.relname IN (%s) AND c.relkind = 'r';`

func (sg *SnapshotGenerator) getSnapshotSchemaTotalBytes(ctx context.Context, snapshotID, schema string, tables []string) (int64, error) {
	paramRefs := make([]string, 0, len(tables))
	tableParams := make([]any, 0, len(tables))
	for i, table := range tables {
		tableParams = append(tableParams, table)
		paramRefs = append(paramRefs, fmt.Sprintf("$%d", i+1))
	}

	totalBytes := int64(0)
	query := fmt.Sprintf(tablesBytesQuery, schema, strings.Join(paramRefs, ","))
	sg.logger.Debug("querying total bytes for schema", loglib.Fields{
		"schema": schema, "tables": tables, "query": query, "snapshotID": snapshotID,
	})
	err := sg.execInSnapshotTx(ctx, snapshotID, func(tx pglib.Tx) error {
		err := tx.QueryRow(ctx, query, tableParams...).Scan(&totalBytes)
		if err != nil {
			return fmt.Errorf("retrieving total bytes for schema: %w", err)
		}
		return nil
	})

	return totalBytes, err
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

func (t *tableInfo) getBatchPageSize(bytes uint64) uint {
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

func (t *tableInfo) isEmpty() bool {
	return t.pageCount < 0
}
