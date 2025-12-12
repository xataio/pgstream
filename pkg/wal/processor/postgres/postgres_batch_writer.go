// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// BatchWriter is a WAL processor implementation that batches and writes wal
// events to a Postgres instance.
type BatchWriter struct {
	*Writer

	batchSender queryBatchSender
}

const batchWriter = "postgres_batch_writer"

var errSchemaLogStoreNotProvided = errors.New("schema log store URL must be provided or DDL events will not be processed. If this is intended, set ignore DDL to true")

// NewBatchWriter returns a postgres processor that batches and writes data to
// the configured postgres instance.
func NewBatchWriter(ctx context.Context, config *Config, opts ...WriterOption) (*BatchWriter, error) {
	var schemaLogStore schemalog.Store
	if !config.IgnoreDDL {
		if config.SchemaLogStore.URL == "" {
			return nil, errSchemaLogStoreNotProvided
		}
		var err error
		schemaLogStore, err = schemalogpg.NewStore(ctx, config.SchemaLogStore)
		if err != nil {
			return nil, fmt.Errorf("create schema log postgres store: %w", err)
		}
		schemaLogStore = schemalog.NewStoreCache(schemaLogStore)
	}

	w, err := newWriter(ctx, config, schemaLogStore, batchWriter, opts...)
	if err != nil {
		return nil, err
	}

	bw := &BatchWriter{
		Writer: w,
	}

	bw.batchSender, err = batch.NewSender(ctx, &config.BatchConfig, bw.sendBatch, w.logger)
	if err != nil {
		return nil, err
	}

	return bw, nil
}

// ProcessWALEvent is called on every new message from the wal. It can be called
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
	return batchWriter
}

func (w *BatchWriter) Close() error {
	w.logger.Debug("closing batch writer")
	w.batchSender.Close()

	return w.close()
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
				if !w.isInternalError(err) {
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
		if err := w.setReplicationRoleToReplica(ctx, tx); err != nil {
			return err
		}

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

		return w.resetReplicationRole(ctx, tx)
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
	var errRelationAlreadyExists *pglib.ErrRelationAlreadyExists
	var errPreconditionFailed *pglib.ErrPreconditionFailed
	switch {
	case errors.As(err, &errRelationDoesNotExist),
		errors.As(err, &errConstraintViolation),
		errors.As(err, &errSyntaxError),
		errors.As(err, &errDataException),
		errors.As(err, &errRelationAlreadyExists),
		errors.As(err, &errPreconditionFailed):
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
