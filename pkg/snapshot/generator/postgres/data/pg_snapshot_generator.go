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

	schemaWorkers uint
	tableWorkers  uint
	batchPageSize uint

	// Function called for processing produced rows.
	processRow             snapshot.RowProcessor
	tableSnapshotGenerator snapshotTableFn
}

type mapper interface {
	TypeForOID(context.Context, uint32) (string, error)
}

type pageRange struct {
	start uint
	end   uint
}

type snapshotTableFn func(ctx context.Context, snapshotID string, schema, table string) error

type Option func(sg *SnapshotGenerator)

func NewSnapshotGenerator(ctx context.Context, cfg *Config, processRow snapshot.RowProcessor, opts ...Option) (*SnapshotGenerator, error) {
	conn, err := pglib.NewConnPool(ctx, cfg.URL)
	if err != nil {
		return nil, err
	}

	sg := &SnapshotGenerator{
		logger:        loglib.NewNoopLogger(),
		mapper:        pglib.NewMapper(conn),
		conn:          conn,
		processRow:    processRow,
		batchPageSize: cfg.batchPageSize(),
		tableWorkers:  cfg.tableWorkers(),
		schemaWorkers: cfg.schemaWorkers(),
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

func (sg *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	// use a transaction snapshot to ensure the table rows can be parallelised.
	// The transaction snapshot is available for use only until the end of the
	// transaction that exported it.
	// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION
	return sg.conn.ExecInTxWithOptions(ctx, func(tx pglib.Tx) error {
		snapshotID, err := sg.exportSnapshot(ctx, tx)
		if err != nil {
			return &snapshot.Errors{SnapshotErrMsgs: []string{err.Error()}}
		}

		tableChan := make(chan string, len(ss.TableNames))
		// a map of table errors per worker to avoid race conditions
		workerTableErrs := make([]map[string]error, sg.schemaWorkers)
		wg := &sync.WaitGroup{}
		// start as many go routines as configured concurrent workers per schema
		for i := uint(0); i < sg.schemaWorkers; i++ {
			wg.Add(1)
			workerTableErrs[i] = make(map[string]error, len(ss.TableNames))
			go sg.createSnapshotWorker(ctx, wg, ss, snapshotID, tableChan, workerTableErrs[i])
		}

		for _, table := range ss.TableNames {
			tableChan <- table
		}

		close(tableChan)
		wg.Wait()

		return sg.collectSnapshotTableErrors(workerTableErrs)
	}, snapshotTxOptions())
}

func (sg *SnapshotGenerator) Close() error {
	return sg.conn.Close(context.Background())
}

func (sg *SnapshotGenerator) createSnapshotWorker(ctx context.Context, wg *sync.WaitGroup, ss *snapshot.Snapshot, snapshotID string, tableChan <-chan string, tableErrMap map[string]error) {
	defer wg.Done()
	for table := range tableChan {
		logFields := loglib.Fields{"schema": ss.SchemaName, "table": table, "snapshotID": snapshotID}
		sg.logger.Info("snapshotting table", logFields)

		if err := sg.tableSnapshotGenerator(ctx, snapshotID, ss.SchemaName, table); err != nil {
			sg.logger.Error(err, "snapshotting table", logFields)
			// errors will get notified unless the table doesn't exist
			if !errors.Is(err, pglib.ErrNoRows) {
				tableErrMap[table] = err
			}
		}
		sg.logger.Info("table snapshot completed", logFields)
	}
}

func (sg *SnapshotGenerator) collectSnapshotTableErrors(workerTableErrs []map[string]error) error {
	var tableErrs []snapshot.TableError
	for _, worker := range workerTableErrs {
		for table, err := range worker {
			tableErrs = append(tableErrs, snapshot.NewTableError(table, err))
		}
	}

	if len(tableErrs) > 0 {
		return &snapshot.Errors{Tables: tableErrs}
	}

	return nil
}

func (sg *SnapshotGenerator) snapshotTable(ctx context.Context, snapshotID string, schema, table string) error {
	tablePageCount, err := sg.getTablePageCount(ctx, schema, table, snapshotID)
	if err != nil {
		return err
	}

	// If one page range fails, we abort the entire table snapshot. The
	// snapshot relies on the transaction snapshot id to ensure all workers
	// have the same table view, which allows us to use the ctid to
	// parallelise the work.
	rangeChan := make(chan pageRange, tablePageCount)
	errGroup, ctx := errgroup.WithContext(ctx)
	for i := uint(0); i < sg.tableWorkers; i++ {
		errGroup.Go(func() error {
			return sg.snapshotTableRangeWorker(ctx, snapshotID, schema, table, rangeChan)
		})
	}

	// page count returned by postgres starts at 0, so we need to include it
	// when creating the page ranges.
	for start := uint(0); start <= tablePageCount; start += sg.batchPageSize {
		rangeChan <- pageRange{
			start: start,
			end:   start + sg.batchPageSize,
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

func (sg *SnapshotGenerator) snapshotTableRange(ctx context.Context, snapshotID, schema, table string, pageRange pageRange) error {
	return sg.execInSnapshotTx(ctx, snapshotID, func(tx pglib.Tx) error {
		sg.logger.Debug(fmt.Sprintf("querying table page range %d-%d", pageRange.start, pageRange.end), loglib.Fields{
			"schema": schema, "table": table, "snapshotID": snapshotID,
		})

		query := fmt.Sprintf("SELECT * FROM %s WHERE ctid BETWEEN '(%d,0)' AND '(%d,0)'",
			pglib.QuoteQualifiedIdentifier(schema, table), pageRange.start, pageRange.end)
		rows, err := tx.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("querying table rows: %w", err)
		}
		defer rows.Close()

		fieldDescriptions := rows.FieldDescriptions()
		for rows.Next() {
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

				if err := sg.processRow(ctx, &snapshot.Row{
					Schema:  schema,
					Table:   table,
					Columns: columns,
				}); err != nil {
					return fmt.Errorf("processing snapshot row: %w", err)
				}
			}
		}

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

func (sg *SnapshotGenerator) getTablePageCount(ctx context.Context, schemaName, tableName, snapshotID string) (uint, error) {
	pageCount := uint(0)
	err := sg.execInSnapshotTx(ctx, snapshotID, func(tx pglib.Tx) error {
		const query = "SELECT c.relpages FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname=$1 and n.nspname=$2"
		if err := tx.QueryRow(ctx, query, tableName, schemaName).Scan(&pageCount); err != nil {
			return fmt.Errorf("getting page count for table %s.%s: %w", schemaName, tableName, err)
		}

		sg.logger.Debug(fmt.Sprintf("table page count: %d", pageCount), loglib.Fields{
			"schema": schemaName, "table": tableName, "snapshotID": snapshotID,
		})
		return nil
	})
	if err != nil {
		return 0, err
	}

	return pageCount, nil
}

func (sg *SnapshotGenerator) exportSnapshot(ctx context.Context, tx pglib.Tx) (string, error) {
	var snapshotID string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
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
