// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"runtime/debug"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// BatchWriter is a WAL processor implementation that batches and writes wal
// events to a Postgres instance.
type BatchWriter struct {
	logger  loglib.Logger
	pgConn  pglib.Querier
	adapter walAdapter

	batchSender queryBatchSender

	// optional checkpointer callback to mark what was safely processed
	checkpointer checkpointer.Checkpoint
}

type Option func(*BatchWriter)

type queryBatchSender interface {
	AddToBatch(context.Context, *batch.WALMessage[*query]) error
	Close()
	Send(context.Context) error
}

// NewBatchWriter returns a postgres processor that batches and writes data to
// the configured postgres instance.
func NewBatchWriter(ctx context.Context, config *Config, opts ...Option) (*BatchWriter, error) {
	pgConn, err := pglib.NewConnPool(ctx, config.URL)
	if err != nil {
		return nil, err
	}

	w := &BatchWriter{
		logger:  loglib.NewNoopLogger(),
		pgConn:  pgConn,
		adapter: &adapter{},
	}

	for _, opt := range opts {
		opt(w)
	}

	w.batchSender, err = batch.NewSender(&config.BatchConfig, w.sendBatch, w.logger)
	if err != nil {
		return nil, err
	}

	// start the send process in the background
	go func() {
		if err := w.batchSender.Send(ctx); err != nil {
			w.logger.Error(err, "sending stopped")
		}
	}()

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

	query, err := w.adapter.walEventToQuery(walEvent)
	if err != nil {
		return err
	}

	msg := batch.NewWALMessage(query, walEvent.CommitPosition)
	return w.batchSender.AddToBatch(ctx, msg)
}

func (w *BatchWriter) Name() string {
	return "postgres-batch-writer"
}

func (w *BatchWriter) Close() error {
	w.batchSender.Close()
	return w.pgConn.Close(context.Background())
}

func (w *BatchWriter) sendBatch(ctx context.Context, batch *batch.Batch[*query]) error {
	err := w.pgConn.ExecInTx(ctx, func(tx pglib.Tx) error {
		for _, q := range batch.GetMessages() {
			if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if w.checkpointer != nil {
		return w.checkpointer(ctx, batch.GetCommitPositions())
	}

	return nil
}
