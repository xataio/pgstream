// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibretrier "github.com/xataio/pgstream/internal/postgres/retrier"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Writer struct {
	logger          loglib.Logger
	pgConn          pglib.Querier
	adapter         walAdapter
	checkpointer    checkpointer.Checkpoint
	writerType      string
	disableTriggers bool
}

type queryBatchSender interface {
	SendMessage(context.Context, *batch.WALMessage[*query]) error
	Close()
}

type WriterOption func(*Writer)

func newWriter(ctx context.Context, config *Config, writerType string, opts ...WriterOption) (*Writer, error) {
	w := &Writer{
		logger:          loglib.NewNoopLogger(),
		writerType:      writerType,
		disableTriggers: config.DisableTriggers,
	}

	for _, opt := range opts {
		opt(w)
	}

	var err error
	if config.RetryPolicy.DisableRetries {
		w.pgConn, err = pglib.NewConnPool(ctx, config.URL)
	} else {
		// unless retries are disabled, wrap the Postgres querier with a retrier
		// and apply default retry policy if none is set
		w.pgConn, err = pglibretrier.NewQuerier(ctx, config.retryPolicy(), func(ctx context.Context) (pglib.Querier, error) {
			return pglib.NewConnPool(ctx, config.URL)
		}, w.logger)
	}
	if err != nil {
		return nil, err
	}

	forCopy := writerType == bulkIngestWriter

	w.adapter, err = newAdapter(ctx, w.logger, config, forCopy)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Writer) close() error {
	if err := w.adapter.close(); err != nil {
		w.logger.Error(err, "closing adapter")
	}
	return w.pgConn.Close(context.Background())
}

func WithLogger(l loglib.Logger) WriterOption {
	return func(w *Writer) {
		w.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: w.writerType,
		})
	}
}

func WithCheckpoint(c checkpointer.Checkpoint) WriterOption {
	return func(w *Writer) {
		w.checkpointer = c
	}
}

func WithInstrumentation(i *otel.Instrumentation) WriterOption {
	return func(w *Writer) {
		w.adapter = newInstrumentedWalAdapter(w.adapter, i)
	}
}

func (w *Writer) setReplicationRoleToReplica(ctx context.Context, tx pglib.Tx) error {
	if !w.disableTriggers {
		return nil
	}

	_, err := tx.Exec(ctx, "SET session_replication_role = replica")
	if err != nil {
		return fmt.Errorf("disabling triggers on postgres instance: %w", err)
	}
	return nil
}

func (w *Writer) resetReplicationRole(ctx context.Context, tx pglib.Tx) error {
	if !w.disableTriggers {
		return nil
	}

	if _, err := tx.Exec(ctx, "SET session_replication_role = DEFAULT"); err != nil {
		return fmt.Errorf("resetting session replication role to default: %w", err)
	}
	return nil
}
