// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/pkg/kafka"
	kafkainstrumentation "github.com/xataio/pgstream/pkg/kafka/instrumentation"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	pgcheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/postgres"
	"github.com/xataio/pgstream/pkg/wal/listener"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	pglistener "github.com/xataio/pgstream/pkg/wal/listener/postgres"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationinstrumentation "github.com/xataio/pgstream/pkg/wal/replication/instrumentation"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
	replicationretrier "github.com/xataio/pgstream/pkg/wal/replication/retrier"

	"golang.org/x/sync/errgroup"
)

// Run will run the configured pgstream processes. This call is blocking.
func Run(ctx context.Context, logger loglib.Logger, config *Config, init bool, instrumentation *otel.Instrumentation) error {
	if err := config.IsValid(); err != nil {
		return fmt.Errorf("incompatible configuration: %w", err)
	}

	if init {
		if err := Init(ctx, config.SourcePostgresURL(), config.PostgresReplicationSlot()); err != nil {
			return err
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	var replicationHandler replication.Handler
	if config.Listener.Postgres != nil {
		var err error
		replicationHandler, err = pgreplication.NewHandler(ctx,
			config.Listener.Postgres.Replication,
			pgreplication.WithLogger(logger))
		if err != nil {
			return fmt.Errorf("error setting up postgres replication handler: %w", err)
		}
		defer replicationHandler.Close()
		// add retry layer to the replication handler
		replicationHandler = replicationretrier.NewHandler(
			replicationHandler,
			config.Listener.Postgres.RetryPolicy,
			replicationretrier.WithLogger(logger))
	}

	if replicationHandler != nil && instrumentation.IsEnabled() {
		var err error
		replicationHandler, err = replicationinstrumentation.NewHandler(replicationHandler, instrumentation)
		if err != nil {
			return err
		}
	}

	var kafkaReader kafka.MessageReader
	if config.Listener.Kafka != nil {
		var err error
		kafkaReader, err = kafka.NewReader(config.Listener.Kafka.Reader, logger)
		if err != nil {
			return fmt.Errorf("error setting up kafka reader: %w", err)
		}
		defer kafkaReader.Close()
	}

	if kafkaReader != nil && instrumentation.IsEnabled() {
		var err error
		kafkaReader, err = kafkainstrumentation.NewReader(kafkaReader, instrumentation)
		if err != nil {
			return err
		}
	}

	// Checkpointer

	var checkpoint checkpointer.Checkpoint
	switch {
	case config.Listener.Kafka != nil:
		kafkaCheckpointer, err := kafkacheckpoint.New(ctx,
			config.Listener.Kafka.Checkpointer,
			kafkaReader,
			kafkacheckpoint.WithLogger(logger))
		if err != nil {
			return fmt.Errorf("error setting up kafka checkpointer:%w", err)
		}
		defer kafkaCheckpointer.Close()
		checkpoint = kafkaCheckpointer.CommitOffsets

	case config.Listener.Postgres != nil:
		pgCheckpointer := pgcheckpoint.New(replicationHandler)
		defer pgCheckpointer.Close()
		checkpoint = pgCheckpointer.SyncLSN
	}

	// Processor

	processor, closer, err := newProcessor(ctx, logger, config, checkpoint, processorTypeReplication, instrumentation)
	defer closer()
	if err != nil {
		return err
	}

	// Listener

	var listener listener.Listener
	switch {
	case config.Listener.Postgres != nil:
		logger.Info("postgres listener configured")
		opts := []pglistener.Option{
			pglistener.WithLogger(logger),
		}
		if config.Listener.Postgres.Snapshot != nil {
			logger.Info("initial snapshot enabled")
			// use a dedicated processor for the snapshot phase, to be able to
			// close it and make sure the snapshot is complete before starting
			// to process the WAL replication events.
			snapshotProcessor, snapshotCloser, err := newProcessor(ctx, logger, config, checkpoint, processorTypeSnapshot, instrumentation)
			defer snapshotCloser()
			if err != nil {
				return fmt.Errorf("error creating snapshot processor: %w", err)
			}

			snapshotGenerator, err := snapshotbuilder.NewSnapshotGenerator(
				ctx,
				config.Listener.Postgres.Snapshot,
				snapshotProcessor,
				logger,
				instrumentation)
			if err != nil {
				return err
			}
			defer snapshotGenerator.Close()
			opts = append(opts, pglistener.WithInitialSnapshot(snapshotGenerator))
		}

		listener = pglistener.New(
			replicationHandler,
			processor.ProcessWALEvent,
			opts...)
	case config.Listener.Kafka != nil:
		logger.Info("kafka listener configured")
		listener, err = kafkalistener.NewWALReader(
			kafkaReader,
			processor.ProcessWALEvent,
			kafkalistener.WithLogger(logger))
		if err != nil {
			return err
		}
	default:
		return errors.New("no supported listener found")
	}
	defer listener.Close()

	eg.Go(func() error {
		defer logger.Info("stopping listener...")
		logger.Info("starting listener...")
		return listener.Listen(ctx)
	})

	if err := eg.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	return nil
}

var noopCloser func() error = func() error {
	return nil
}

func newProcessor(ctx context.Context, logger loglib.Logger, config *Config, checkpoint checkpointer.Checkpoint, processorType processorType, instrumentation *otel.Instrumentation) (processor.Processor, closerFn, error) {
	processor, err := buildProcessor(ctx, logger, &config.Processor, checkpoint, processorType, instrumentation)
	if err != nil {
		return nil, noopCloser, err
	}
	var closerAgg closerAggregator
	var closer closerFn
	processor, closer, err = addProcessorModifiers(ctx, config, logger, processor, instrumentation)
	if err != nil {
		return nil, noopCloser, err
	}

	closerAgg.addCloserFn(closer)
	closerAgg.addCloserFn(processor.Close)

	return processor, closerAgg.close, nil
}
