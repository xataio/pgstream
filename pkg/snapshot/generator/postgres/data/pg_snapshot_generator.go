// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibinstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	"github.com/xataio/pgstream/internal/progress"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"golang.org/x/sync/errgroup"
)

type SnapshotGenerator struct {
	logger    loglib.Logger
	conn      pglib.Querier
	processor processor.Processor
	// reader encapsulates the strategy used to read a schema's tables (ctid
	// range scan by default).
	reader tableReader
	// instrumentation is captured while applying options and used to wrap the
	// reader once it has been built.
	instrumentation *otel.Instrumentation

	// workers per snapshot, parallelise the snapshot creation for each schema
	snapshotWorkers uint
	// workers per schema, parallelise the snapshot creation for each table
	schemaWorkers uint

	progress           progressTracker
	progressBarBuilder func(totalBytes int64, description string) progress.Bar
}

type mapper interface {
	TypeForOID(context.Context, uint32) (string, error)
}

type tableInfo struct {
	pageCount     int
	avgPageBytes  int64
	avgRowBytes   int64
	batchPageSize uint
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

type Option func(sg *SnapshotGenerator)

func NewSnapshotGenerator(ctx context.Context, cfg *Config, processor processor.Processor, opts ...Option) (*SnapshotGenerator, error) {
	poolOpts := []pglib.PoolOption{pglib.WithMaxConnections(int32(cfg.maxConnections()))}
	if cfg.RawJSONValues {
		poolOpts = append(poolOpts, pglib.WithRawJSONDecoding())
	}
	conn, err := pglib.NewConnPool(ctx, cfg.URL, poolOpts...)
	if err != nil {
		return nil, err
	}

	sg := &SnapshotGenerator{
		logger:          loglib.NewNoopLogger(),
		conn:            conn,
		processor:       processor,
		schemaWorkers:   cfg.schemaWorkers(),
		snapshotWorkers: cfg.snapshotWorkers(),
	}

	for _, opt := range opts {
		opt(sg)
	}

	sg.reader = &ctidReader{
		conn:         sg.conn,
		logger:       sg.logger,
		adapter:      newAdapter(pglib.NewMapper(conn), sg.logger),
		processor:    sg.processor,
		tableWorkers: cfg.tableWorkers(),
		batchBytes:   cfg.batchBytes(),
		progress:     sg.progress,
	}

	if sg.instrumentation != nil {
		sg.reader = newInstrumentedTableReader(sg.reader, sg.instrumentation)
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

		sg.instrumentation = i
	}
}

func WithProgressTracking() Option {
	return func(sg *SnapshotGenerator) {
		sg.progress = newProgressTracker()
		sg.progressBarBuilder = progress.NewBytesBar
	}
}

func (sg *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) (err error) {
	defer func() {
		// make sure we close the processor once the snapshot is completed.
		// It will wait until all rows are processed before returning.
		if closeErr := sg.processor.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = errors.Join(err, closeErr)
			}
		}
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
	return sg.reader.beginSchema(ctx, schemaTables, func(ctx context.Context, session *readSession) (err error) {
		if sg.progress.enabled {
			if err := sg.addProgressBar(ctx, session.snapshotID, schemaTables); err != nil {
				return err
			}
			defer func() {
				if err == nil {
					sg.progress.complete(schemaTables.schema)
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
			go sg.createSnapshotWorker(ctx, wg, session, tableChan, workerTableErrs[i])
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
	})
}

func (sg *SnapshotGenerator) createSnapshotWorker(ctx context.Context, wg *sync.WaitGroup, session *readSession, tableChan <-chan *table, tableErrMap map[string]error) {
	defer wg.Done()
	for t := range tableChan {
		logFields := loglib.Fields{"schema": t.schema, "table": t.name, "snapshotID": session.snapshotID}
		sg.logger.Debug("snapshotting table", logFields)

		if err := sg.reader.readTable(ctx, session, t); err != nil {
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

func (sg *SnapshotGenerator) addProgressBar(ctx context.Context, snapshotID string, schemaTables *schemaTables) error {
	totalBytes, err := sg.getSnapshotSchemaTotalBytes(ctx, snapshotID, schemaTables.schema, schemaTables.tables)
	if err != nil {
		return err
	}

	bar := sg.progressBarBuilder(totalBytes, fmt.Sprintf("[cyan][%s][reset] Snapshotting data...", schemaTables.schema))
	sg.progress.set(schemaTables.schema, bar)
	return nil
}

const tablesBytesQuery = `SELECT SUM(pg_table_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = ANY($2) AND c.relkind = 'r';`

func (sg *SnapshotGenerator) getSnapshotSchemaTotalBytes(ctx context.Context, snapshotID, schema string, tables []string) (int64, error) {
	totalBytes := int64(0)
	sg.logger.Debug("querying total bytes for schema", loglib.Fields{
		"schema": schema, "tables": tables, "snapshotID": snapshotID,
	})

	// make sure the schema and table names are unquoted since the system
	// catalogs store unquoted names
	unquotedTables := make([]string, len(tables))
	for i, table := range tables {
		unquotedTables[i] = pglib.UnquoteIdentifier(table)
	}

	err := execInSnapshotTx(ctx, sg.conn, snapshotID, func(tx pglib.Tx) error {
		err := tx.QueryRow(ctx, []any{&totalBytes}, tablesBytesQuery, pglib.UnquoteIdentifier(schema), unquotedTables)
		if err != nil {
			return fmt.Errorf("retrieving total bytes for schema: %w", err)
		}
		return nil
	})

	return totalBytes, err
}

// calculateBatchPageSize will automatically determine the batch page size based
// on the average page size and the configured batch bytes limit.
func (t *tableInfo) calculateBatchPageSize(bytes uint64) {
	// at least one page is needed to process the table
	if t.pageCount == 0 {
		t.batchPageSize = 1
		return
	}

	// no limit on bytes, return all pages
	if bytes == 0 || t.avgPageBytes == 0 {
		t.batchPageSize = uint(t.pageCount)
		return
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

	t.batchPageSize = uint(batchPageSize)
}

func (t *tableInfo) isEmpty() bool {
	return t.pageCount < 0
}
