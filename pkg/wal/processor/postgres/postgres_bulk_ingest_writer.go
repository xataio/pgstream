// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	synclib "github.com/xataio/pgstream/internal/sync"
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

	batchSenderMap     *synclib.Map[string, queryBatchSender]
	batchSenderBuilder func(ctx context.Context, schema, table string) (queryBatchSender, error)
}

const bulkIngestWriter = "postgres_bulk_ingest_writer"

var errUnexpectedCopiedRows = errors.New("number of rows copied doesn't match the source rows")

// NewBulkIngestWriter returns a postgres processor that batches and writes data
// to the configured postgres instance in bulk using the COPY command. It uses a
// batch sender per schema table to parallelise the bulk ingest for different
// tables.
func NewBulkIngestWriter(ctx context.Context, config *Config, opts ...WriterOption) (*BulkIngestWriter, error) {
	// the bulk ingest writer only processes insert events, so we don't need a
	// DDL adapter
	config.IgnoreDDL = true
	w, err := newWriter(ctx, config, bulkIngestWriter, opts...)
	if err != nil {
		return nil, err
	}

	biw := &BulkIngestWriter{
		Writer:         w,
		batchSenderMap: synclib.NewMap[string, queryBatchSender](),
	}

	biw.batchSenderBuilder = func(ctx context.Context, schema, table string) (queryBatchSender, error) {
		logger := w.logger.WithFields(loglib.Fields{"schema": schema, "table": table})
		return batch.NewSender(ctx, &config.BatchConfig, biw.sendBatch, logger)
	}

	return biw, nil
}

// ProcessWALEvent is called on every new message from the wal. It can be called
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
	for _, sender := range w.batchSenderMap.GetMap() {
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

	batchSender, found := w.batchSenderMap.Get(key)
	if found {
		return batchSender, nil
	}

	w.logger.Debug("creating new batch sender", loglib.Fields{"schema": schema, "table": table})
	sender, err := w.batchSenderBuilder(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	w.batchSenderMap.Set(key, sender)
	return sender, nil
}

func (w *BulkIngestWriter) sendBatch(ctx context.Context, batch *batch.Batch[*query]) error {
	queries := batch.GetMessages()
	if len(queries) == 0 {
		return nil
	}

	w.logger.Trace("bulk writing batch", loglib.Fields{"batch_size": len(queries)})
	return w.copyFromInsertQueries(ctx, queries)
}

func (w *BulkIngestWriter) copyFromInsertQueries(ctx context.Context, inserts []*query) error {
	if len(inserts) == 0 {
		return nil
	}

	// Get the table and column names from the first insert query, since all the
	// inserts in the batch will be for the same schema.table
	query := inserts[0]
	rows := [][]any{}
	for _, q := range inserts {
		rows = append(rows, q.args)
	}

	err := w.pgConn.ExecInTx(ctx, func(tx pglib.Tx) error {
		if err := w.setReplicationRoleToReplica(ctx, tx); err != nil {
			return err
		}

		rowsCopied, err := tx.CopyFrom(ctx, pglib.QuoteQualifiedIdentifier(query.schema, query.table), query.columnNames, rows)
		if err != nil {
			return err
		}

		rowsToCopy := len(inserts)
		if rowsCopied != int64(rowsToCopy) {
			return fmt.Errorf("%w: copied (%d), expected(%d)", errUnexpectedCopiedRows, rowsCopied, rowsToCopy)
		}

		return w.resetReplicationRole(ctx, tx)
	})
	if err != nil {
		// COPY binary fails for extension types (cube, vector, etc.) that
		// lack binary encoders in pgx. Detect encoding errors and fall back
		// to INSERT statements which use text format.
		// Other errors (connection, replication role, row count mismatch)
		// should propagate normally.
		if isCopyEncodingError(err) {
			w.logger.Warn(err, "COPY failed due to type encoding, falling back to INSERT", loglib.Fields{
				"schema": query.schema,
				"table":  query.table,
				"rows":   len(inserts),
			})
			return w.insertFallback(ctx, inserts)
		}
		return fmt.Errorf("copy from: %w", err)
	}
	return nil
}

// insertFallback executes INSERT statements one at a time when COPY binary
// fails. This handles extension types (cube, vector, etc.) that pgx can't
// encode in binary format but can encode in text format via parameterized
// INSERT.
func (w *BulkIngestWriter) insertFallback(ctx context.Context, inserts []*query) error {
	return w.pgConn.ExecInTx(ctx, func(tx pglib.Tx) error {
		if err := w.setReplicationRoleToReplica(ctx, tx); err != nil {
			return err
		}

		for _, q := range inserts {
			if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
				w.logger.Error(err, "INSERT fallback failed", loglib.Fields{
					"sql":  q.sql,
					"args": q.args,
				})
				return err
			}
		}

		return w.resetReplicationRole(ctx, tx)
	})
}

// isCopyEncodingError returns true if the error is from pgx failing to encode
// a value for COPY binary format. This happens with extension types (cube,
// vector, etc.) that lack registered binary codecs.
func isCopyEncodingError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "cannot find encode plan") ||
		strings.Contains(msg, "unable to encode") ||
		strings.Contains(msg, "dimension is too large") ||
		strings.Contains(msg, "cannot have more than")
}
