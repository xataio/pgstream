// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"golang.org/x/sync/errgroup"
)

// BulkIngestWriter is a WAL processor implementation that batches and bulk
// writes wal insert events to a Postgres instance using the COPY command.
type BulkIngestWriter struct {
	*Writer

	batchSenderMapMutex *sync.RWMutex
	batchSenderMap      map[string]queryBatchSender
	batchSenderBuilder  func(ctx context.Context) (queryBatchSender, error)
}

const bulkIngestWriter = "postgres_bulk_ingest_writer"

// NewBulkIngestWriter returns a postgres processor that batches and writes data
// to the configured postgres instance in bulk using the COPY command. It uses a
// batch sender per schema table to parallelise the bulk ingest for different
// tables.
func NewBulkIngestWriter(ctx context.Context, config *Config, opts ...WriterOption) (*BulkIngestWriter, error) {
	// the bulk ingest writer only processes insert events, so we don't need a
	// DDL adapter
	adapter, err := newAdapter(nil, config.OnConflictAction)
	if err != nil {
		return nil, err
	}
	// disable triggers to prevent FK constraints from firing during bulk inserts
	config.DisableTriggers = true
	w, err := newWriter(ctx, config, adapter, bulkIngestWriter, opts...)
	if err != nil {
		return nil, err
	}

	biw := &BulkIngestWriter{
		Writer:              w,
		batchSenderMapMutex: &sync.RWMutex{},
		batchSenderMap:      make(map[string]queryBatchSender),
	}

	biw.batchSenderBuilder = func(ctx context.Context) (queryBatchSender, error) {
		return batch.NewSender(ctx, &config.BatchConfig, biw.sendBatch, w.logger)
	}

	return biw, nil
}

// ProcessWalEvent is called on every new message from the wal. It can be called
// concurrently.
func (w *BulkIngestWriter) ProcessWALEvent(ctx context.Context, walEvent *wal.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Panic("[PANIC] Panic while processing replication event", loglib.Fields{
				"wal_data":    walEvent,
				"panic":       r,
				"stack_trace": debug.Stack(),
			})

			err = fmt.Errorf("postgres writer: understanding event: %w:  %v", processor.ErrPanic, r)
		}
	}()

	if walEvent.Data != nil && !walEvent.Data.IsInsert() {
		w.logger.Warn(nil, "skipping non-insert event", loglib.Fields{"severity": "DATALOSS"})
		return nil
	}

	queries, err := w.adapter.walEventToQueries(ctx, walEvent)
	if err != nil {
		return err
	}

	for _, q := range queries {
		logFields := loglib.Fields{
			"schema":          q.schema,
			"table":           q.table,
			"commit_position": walEvent.CommitPosition,
			"sql":             q.getSQL(),
			"args":            q.getArgs(),
		}

		w.logger.Trace("batching query", logFields)
		msg := batch.NewWALMessage(q, walEvent.CommitPosition)

		batchSender, err := w.getBatchSender(ctx, q.schema, q.table)
		if err != nil {
			return err
		}

		if err := batchSender.SendMessage(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

func (w *BulkIngestWriter) Name() string {
	return bulkIngestWriter
}

func (w *BulkIngestWriter) Close() error {
	w.logger.Debug("closing bulk ingest writer")

	eg := errgroup.Group{}
	for _, sender := range w.batchSenderMap {
		eg.Go(func() error {
			sender.Close()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		w.logger.Error(err, "closing batch senders")
	}

	return w.close()
}

func (w *BulkIngestWriter) getBatchSender(ctx context.Context, schema, table string) (queryBatchSender, error) {
	key := pglib.QuoteQualifiedIdentifier(schema, table)

	w.batchSenderMapMutex.RLock()
	sender, found := w.batchSenderMap[key]
	w.batchSenderMapMutex.RUnlock()
	if found {
		return sender, nil
	}

	w.logger.Debug("creating new batch sender", loglib.Fields{"schema": schema, "table": table})
	sender, err := w.batchSenderBuilder(ctx)
	if err != nil {
		return nil, err
	}

	w.batchSenderMapMutex.Lock()
	w.batchSenderMap[key] = sender
	w.batchSenderMapMutex.Unlock()

	return sender, nil
}

func (w *BulkIngestWriter) sendBatch(ctx context.Context, batch *batch.Batch[*query]) error {
	queries := batch.GetMessages()
	if len(queries) == 0 {
		return nil
	}

	w.logger.Debug("bulk writing batch", loglib.Fields{"batch_size": len(queries)})
	return w.copyFromInsertQueries(ctx, queries)
}

func (w *BulkIngestWriter) copyFromInsertQueries(ctx context.Context, inserts []*query) error {
	if len(inserts) == 0 {
		return nil
	}
	// Get the column names from the first insert query, since all the inserts
	// in the batch will be for the same schema.table
	query := inserts[0]
	srcRows := [][]any{}
	for _, q := range inserts {
		srcRows = append(srcRows, q.args)
	}

	rowsToCopy := len(inserts)
	rowsCopied, err := w.pgConn.CopyFrom(ctx, pglib.QuoteQualifiedIdentifier(query.schema, query.table), query.columnNames, srcRows)
	if err != nil {
		return err
	}

	if rowsCopied != int64(rowsToCopy) {
		return fmt.Errorf("number of rows copied (%d) doesn't match the source rows (%d)", rowsCopied, rowsToCopy)
	}

	return nil
}
