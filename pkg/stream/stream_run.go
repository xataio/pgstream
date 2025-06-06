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
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationinstrumentation "github.com/xataio/pgstream/pkg/wal/replication/instrumentation"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"

	"golang.org/x/sync/errgroup"
)

// Run will run the configured pgstream processes. This call is blocking.
func Run(ctx context.Context, logger loglib.Logger, config *Config, instrumentation *otel.Instrumentation) error {
	if err := config.IsValid(); err != nil {
		return fmt.Errorf("incompatible configuration: %w", err)
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

	processor, err := buildProcessor(ctx, logger, &config.Processor, checkpoint, instrumentation)
	if err != nil {
		return err
	}
	defer processor.Close()

	var closer closerFn
	processor, closer, err = addProcessorModifiers(ctx, config, logger, processor, instrumentation)
	if err != nil {
		return err
	}
	defer closer()

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
			snapshotGenerator, err := snapshotbuilder.NewSnapshotGenerator(
				ctx,
				config.Listener.Postgres.Snapshot,
				processor,
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
