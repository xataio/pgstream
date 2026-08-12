// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

const allColumns = "*"

// ErrSchemaChangedDuringSnapshot: re-snapshot needed.
var ErrSchemaChangedDuringSnapshot = errors.New("source schema changed during the snapshot")

type SnapshotGenerator struct {
	logger    loglib.Logger
	conn      pglib.Querier
	processor processor.Processor
	// reader encapsulates the strategy used to read a schema's tables (ctid
	// range scan by default).
	reader tableReader
	// instrumentation is captured while applying options and used to decorate
	// the reader once it has been built.
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
	columns       []string
}

type pageRange struct {
	start uint
	end   uint
}

type schemaTables struct {
	schema string
	tables []string
	// nil without a schema snapshot
	columns pglib.SchemaTableColumns
}

type table struct {
	schema  string
	name    string
	rowSize int64
	// one list per page range
	columns []string
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

	sink := newRowSink(pglib.NewMapper(conn), sg.processor, sg.logger, sg.progress)
	sg.reader = newTableReader(sg.conn, sg.logger, sink, cfg, sg.instrumentation)

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
			schema:  schema,
			tables:  tables,
			columns: ss.TableColumns,
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
	return sg.reader.beginSchema(ctx, schemaTables, func(ctx context.Context, session readSession) (err error) {
		if sg.progress.enabled {
			if err := sg.addProgressBar(ctx, session, schemaTables); err != nil {
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
				schema:  schemaTables.schema,
				name:    tableName,
				columns: sg.pinnedColumns(schemaTables, tableName),
			}
		}

		close(tableChan)
		wg.Wait()

		return sg.collectTableErrors(schemaTables.schema, workerTableErrs)
	})
}

// pinnedColumns warns on uncaptured tables.
func (sg *SnapshotGenerator) pinnedColumns(schemaTables *schemaTables, tableName string) []string {
	if schemaTables.columns == nil {
		return nil
	}

	columns := schemaTables.columns.ColumnsFor(schemaTables.schema, tableName)
	if len(columns) == 0 {
		sg.logger.Warn(nil, "no columns captured by the schema snapshot for this table, falling back to the columns it has now: it was created after the capture, or its name did not resolve to a catalog entry",
			loglib.Fields{"schema": schemaTables.schema, "table": tableName})
	}
	return columns
}

// readableColumns intersects; drops don't abort.
// The flag reports a pin with nothing left.
func readableColumns(pinned, live []string) ([]string, bool) {
	if len(pinned) == 0 {
		return live, false
	}

	liveSet := make(map[string]struct{}, len(live))
	for _, column := range live {
		liveSet[column] = struct{}{}
	}

	readable := make([]string, 0, len(pinned))
	for _, column := range pinned {
		if _, found := liveSet[column]; found {
			readable = append(readable, column)
		}
	}
	return readable, len(readable) == 0
}

func (sg *SnapshotGenerator) createSnapshotWorker(ctx context.Context, wg *sync.WaitGroup, session readSession, tableChan <-chan *table, tableErrMap map[string]error) {
	defer wg.Done()
	sessionFields := session.logFields()
	for t := range tableChan {
		logFields := loglib.Fields{"schema": t.schema, "table": t.name}
		maps.Copy(logFields, sessionFields)
		sg.logger.Debug("snapshotting table", logFields)

		if err := session.readTable(ctx, t); err != nil {
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

// addProgressBar sizes the schema's progress bar with the total bytes the read
// session reports for it. How those bytes are measured is the reading
// strategy's business; the generator only owns the bar.
func (sg *SnapshotGenerator) addProgressBar(ctx context.Context, session readSession, schemaTables *schemaTables) error {
	totalBytes, err := session.totalBytes(ctx)
	if err != nil {
		return err
	}

	bar := sg.progressBarBuilder(totalBytes, fmt.Sprintf("[cyan][%s][reset] Snapshotting data...", schemaTables.schema))
	sg.progress.set(schemaTables.schema, bar)
	return nil
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
