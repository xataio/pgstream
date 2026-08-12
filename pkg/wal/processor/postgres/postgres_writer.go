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
	maxConnections  int32

	droppedQueries       atomic.Uint64
	instrumentation      *otel.Instrumentation
	droppedQueriesMetric metric.Int64ObservableCounter

	// dropped is shared with every batch sender this writer builds, so the
	// totals and metrics span the writer rather than a single table.
	dropped *batch.DroppedCounter
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
	poolOpts := config.poolOptions()
	maxConnections, err := pglib.ConnPoolMaxConnections(config.URL, poolOpts...)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		logger:          loglib.NewNoopLogger(),
		writerType:      writerType,
		disableTriggers: config.DisableTriggers,
		strictMode:      config.StrictMode,
		maxConnections:  maxConnections,
		dropped:         batch.NewDroppedCounter(),
	}

	for _, opt := range opts {
		opt(w)
	}

	// State the suppression posture where the flags live, rather than at stream
	// assembly: both of these turn a write failure into silently missing rows,
	// are easy to set once and forget, and are otherwise only visible by
	// counting log lines after the fact.
	if config.BatchConfig.IgnoreSendErrors {
		w.logger.Warn(nil, "ignore_send_errors is enabled: batches that fail to send will be dropped and the run will continue", loglib.Fields{
			"posture":     "at_risk",
			"writer_type": writerType,
		})
	}
	// strict mode only governs the per-query drop-and-continue path, which the
	// bulk ingest writer never reaches, so saying so there would describe
	// behaviour the running writer does not have.
	if !config.StrictMode && writerType == batchWriter {
		w.logger.Info("strict_mode is disabled: non-internal query failures will be dropped and counted rather than stopping the pipeline")
	}

	if config.RetryPolicy.DisableRetries {
		w.pgConn, err = pglib.NewConnPool(ctx, config.URL, poolOpts...)
	} else {
		// unless retries are disabled, wrap the Postgres querier with a retrier
		// and apply default retry policy if none is set
		w.pgConn, err = pglibretrier.NewQuerier(ctx, config.retryPolicy(), func(ctx context.Context) (pglib.Querier, error) {
			return pglib.NewConnPool(ctx, config.URL, poolOpts...)
		}, w.logger)
	}
	if err != nil {
		return nil, err
	}

	forCopy := writerType == bulkIngestWriter

	w.adapter, err = newAdapter(ctx, w.logger, config, forCopy, w.maxConnections)
	if err != nil {
		return nil, err
	}

	if w.instrumentation.IsEnabled() {
		w.adapter = newInstrumentedWalAdapter(w.adapter, w.instrumentation)
		if err := w.dropped.RegisterMetrics(w.instrumentation, writerType); err != nil {
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

// initDroppedQueriesMetric registers the per-query drop counter. Only the batch
// writer reaches recordDroppedQuery, so only the batch writer registers this:
// exporting it for the bulk ingest writer too would publish a permanent zero
// under a writer_type label naming a component that does silently drop data, by
// whole batches. Called by NewBatchWriter rather than by the shared
// constructor, so the restriction is structural rather than a type check.
func (w *Writer) initDroppedQueriesMetric() error {
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
// recordDroppedQuery reports a query the writer gave up on. cause is what made
// it undeliverable: without it the DATALOSS warning says what diverged but not
// why, leaving an operator to correlate by timestamp against the error logged
// where the query failed. schema and table are surfaced as their own fields so
// the diverging table can be alerted on without parsing the SQL.
func (w *Writer) recordDroppedQuery(q *query, cause error) {
	dropped := w.droppedQueries.Add(1)
	w.logger.Warn(cause, "dropping failed query and advancing checkpoint, replica may diverge", loglib.Fields{
		"sql":             q.sql,
		"args":            q.args,
		"schema":          q.schema,
		"table":           q.table,
		"severity":        "DATALOSS",
		"dropped_queries": dropped,
	})
}

func (w *Writer) close() error {
	w.dropped.LogTotals(w.logger)
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
