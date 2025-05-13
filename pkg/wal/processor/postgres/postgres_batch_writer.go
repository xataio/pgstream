// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// BatchWriter is a WAL processor implementation that batches and writes wal
// events to a Postgres instance.
type BatchWriter struct {
	logger          loglib.Logger
	pgConn          pglib.Querier
	adapter         walAdapter
	disableTriggers bool

	batchSender queryBatchSender

	// optional checkpointer callback to mark what was safely processed
	checkpointer checkpointer.Checkpoint
}

type Option func(*BatchWriter)

type queryBatchSender interface {
	SendMessage(context.Context, *batch.WALMessage[*query]) error
	Close()
}

// NewBatchWriter returns a postgres processor that batches and writes data to
// the configured postgres instance.
func NewBatchWriter(ctx context.Context, config *Config, opts ...Option) (*BatchWriter, error) {
	pgConn, err := pglib.NewConnPool(ctx, config.URL)
	if err != nil {
		return nil, err
	}

	var schemaLogStore schemalog.Store
	if config.SchemaLogStore.URL != "" {
		schemaLogStore, err = schemalogpg.NewStore(ctx, config.SchemaLogStore)
		if err != nil {
			return nil, fmt.Errorf("create schema log postgres store: %w", err)
		}
		schemaLogStore = schemalog.NewStoreCache(schemaLogStore)
	}

	adapter, err := newAdapter(schemaLogStore, config.OnConflictAction)
	if err != nil {
		return nil, err
	}

	w := &BatchWriter{
		logger:          loglib.NewNoopLogger(),
		pgConn:          pgConn,
		adapter:         adapter,
		disableTriggers: config.DisableTriggers,
	}

	for _, opt := range opts {
		opt(w)
	}

	if w.disableTriggers {
		_, err := pgConn.Exec(ctx, "SET session_replication_role = replica")
		if err != nil {
			return nil, fmt.Errorf("disabling triggers on postgres instance: %w", err)
		}
	}

	w.batchSender, err = batch.NewSender(ctx, &config.BatchConfig, w.sendBatch, w.logger)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(w *BatchWriter) {
		w.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_batch_writer",
		})
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) Option {
	return func(w *BatchWriter) {
		w.checkpointer = c
	}
}

func WithInstrumentation(i *otel.Instrumentation) Option {
	return func(w *BatchWriter) {
		w.adapter = newInstrumentedWalAdapter(w.adapter, i)
	}
}

// ProcessWalEvent is called on every new message from the wal. It can be called
// concurrently.
func (w *BatchWriter) ProcessWALEvent(ctx context.Context, walEvent *wal.Event) (err error) {
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

	queries, err := w.adapter.walEventToQueries(ctx, walEvent)
	if err != nil {
		return err
	}

	for _, q := range queries {
		w.logger.Debug("batching query", loglib.Fields{"sql": q.getSQL(), "args": q.getArgs(), "commit_position": walEvent.CommitPosition})
		msg := batch.NewWALMessage(q, walEvent.CommitPosition)
		if err := w.batchSender.SendMessage(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

func (w *BatchWriter) Name() string {
	return "postgres-batch-writer"
}

func (w *BatchWriter) Close() error {
	w.logger.Debug("closing batch writer")
	w.batchSender.Close()

	ctx := context.Background()
	if w.disableTriggers {
		// reset the replica role once we're done with the batch writer
		if _, err := w.pgConn.Exec(ctx, "SET session_replication_role = DEFAULT"); err != nil {
			w.logger.Error(err, "reseting session replication role to default")
		}
	}
	return w.pgConn.Close(ctx)
}

func (w *BatchWriter) sendBatch(ctx context.Context, batch *batch.Batch[*query]) error {
	queries := batch.GetMessages()
	if len(queries) > 0 {
		w.logger.Debug("sending batch", loglib.Fields{"batch_size": len(queries)})
		// we'll mostly process DML queries, so pre-allocate the max size
		dmlQueries := make([]*query, 0, len(queries))
		for _, q := range queries {
			if !q.isDDL {
				dmlQueries = append(dmlQueries, q)
				continue
			}

			// flush any previous DML queries before running the DDL query to ensure
			// they are run in their own separate transaction
			if err := w.flushQueries(ctx, dmlQueries); err != nil {
				w.logger.Error(err, "flushing DML queries")
				return err
			}

			if _, err := w.pgConn.Exec(ctx, q.sql, q.args...); err != nil {
				w.logger.Error(err, "running DDL query", loglib.Fields{"query_sql": q.sql, "query_args": q.args})
				var errRelationDoesNotExist *pglib.ErrRelationDoesNotExist
				if errors.As(err, &errRelationDoesNotExist) {
					continue
				}
				return err
			}
		}

		if err := w.flushQueries(ctx, dmlQueries); err != nil {
			w.logger.Error(err, "flushing DML queries")
			return err
		}
	}

	if w.checkpointer != nil && len(batch.GetCommitPositions()) > 0 {
		return w.checkpointer(ctx, batch.GetCommitPositions())
	}

	return nil
}

func (w *BatchWriter) flushQueries(ctx context.Context, queries []*query) error {
	if len(queries) == 0 {
		return nil
	}

	var err error
	for {
		queries, err = w.execQueries(ctx, queries)
		if err != nil {
			return err
		}

		if len(queries) == 0 {
			return nil
		}
	}
}

func (w *BatchWriter) execQueries(ctx context.Context, queries []*query) ([]*query, error) {
	retryQueries := []*query{}
	err := w.pgConn.ExecInTx(ctx, func(tx pglib.Tx) error {
		for i, q := range queries {
			if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
				w.logger.Error(err, "executing sql query", loglib.Fields{
					"sql":      q.sql,
					"args":     q.args,
					"severity": "DATALOSS",
				})
				// if a query returns an error, it will abort the tx. Log it as
				// dataloss and remove it from the list of queries to be
				// retried.
				retryQueries = removeIndex(queries, i)
				return err
			}
		}
		return nil
	})
	if err != nil && w.isInternalError(err) {
		// if there was an internal error in the tx, there's no point in
		// retrying, return error and stop processing.
		return nil, err
	}

	// if there were no errors or no internal errors in the tx, return the
	// queries to retry (none if the tx was successful)
	return retryQueries, nil
}

func (w *BatchWriter) isInternalError(err error) bool {
	var errRelationDoesNotExist *pglib.ErrRelationDoesNotExist
	var errConstraintViolation *pglib.ErrConstraintViolation
	var errSyntaxError *pglib.ErrSyntaxError
	var errDataException *pglib.ErrDataException
	switch {
	case errors.As(err, &errRelationDoesNotExist),
		errors.As(err, &errConstraintViolation),
		errors.As(err, &errSyntaxError),
		errors.As(err, &errDataException):
		return false
	default:
		return true
	}
}

func removeIndex(s []*query, index int) []*query {
	ret := make([]*query, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}
