// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"sync/atomic"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibretrier "github.com/xataio/pgstream/internal/postgres/retrier"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Writer struct {
	logger          loglib.Logger
	pgConn          pglib.Querier
	adapter         walAdapter
	checkpointer    checkpointer.Checkpoint
	writerType      string
	disableTriggers bool
	strictMode      bool

	droppedQueries       atomic.Uint64
	instrumentation      *otel.Instrumentation
	droppedQueriesMetric metric.Int64ObservableCounter
}

type queryBatchSender interface {
	SendMessage(context.Context, *batch.WALMessage[*query]) error
	Close() error
}

type walMessageBatchSender interface {
	SendMessage(context.Context, *batch.WALMessage[*walMessage]) error
	Close() error
}

type WriterOption func(*Writer)

func newWriter(ctx context.Context, config *Config, writerType string, opts ...WriterOption) (*Writer, error) {
	w := &Writer{
		logger:          loglib.NewNoopLogger(),
		writerType:      writerType,
		disableTriggers: config.DisableTriggers,
		strictMode:      config.StrictMode,
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

	w.adapter, err = newAdapter(ctx, w.logger, config.IgnoreDDL, config.URL, config.OnConflictAction, forCopy)
	if err != nil {
		return nil, err
	}

	if w.instrumentation.IsEnabled() {
		w.adapter = newInstrumentedWalAdapter(w.adapter, w.instrumentation)
		if err := w.initMetrics(); err != nil {
			return nil, fmt.Errorf("initialising postgres writer metrics: %w", err)
		}
	}

	return w, nil
}

// DroppedQueries returns the total number of queries that have been silently
// dropped due to non-internal (DATALOSS) failures while running in the default
// drop-and-continue mode. It stays at zero when strict mode is enabled.
func (w *Writer) DroppedQueries() uint64 {
	return w.droppedQueries.Load()
}

const droppedQueriesMetricName = "pgstream.postgres.writer.dropped_queries"

func (w *Writer) initMetrics() error {
	if w.instrumentation == nil || w.instrumentation.Meter == nil {
		return nil
	}
	meter := w.instrumentation.Meter

	var err error
	w.droppedQueriesMetric, err = meter.Int64ObservableCounter(droppedQueriesMetricName,
		metric.WithUnit("{query}"),
		metric.WithDescription("Number of queries silently dropped due to non-internal (DATALOSS) failures while running in drop-and-continue mode"))
	if err != nil {
		return err
	}

	_, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(w.droppedQueriesMetric, int64(w.droppedQueries.Load()),
				metric.WithAttributes(attribute.String("writer_type", w.writerType)))
			return nil
		},
		w.droppedQueriesMetric,
	)
	if err != nil {
		return fmt.Errorf("registering postgres writer metric callbacks: %w", err)
	}

	return nil
}

// recordDroppedQuery accounts for a single query dropped due to a non-internal
// (DATALOSS) failure and logs the divergence prominently.
func (w *Writer) recordDroppedQuery(q *query) {
	dropped := w.droppedQueries.Add(1)
	w.logger.Warn(nil, "dropping failed query and advancing checkpoint, replica may diverge", loglib.Fields{
		"sql":             q.sql,
		"args":            q.args,
		"severity":        "DATALOSS",
		"dropped_queries": dropped,
	})
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
		w.instrumentation = i
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
