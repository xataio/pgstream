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
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/data/postgres"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	pgcheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/postgres"
	"github.com/xataio/pgstream/pkg/wal/listener"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	pglistener "github.com/xataio/pgstream/pkg/wal/listener/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	processinstrumentation "github.com/xataio/pgstream/pkg/wal/processor/instrumentation"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	searchinstrumentation "github.com/xataio/pgstream/pkg/wal/processor/search/instrumentation"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	webhooknotifier "github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	subscriptionserver "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	webhookstore "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store"
	subscriptionstorecache "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/cache"
	pgwebhook "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/postgres"
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

	var processor processor.Processor
	switch {
	case config.Processor.Kafka != nil:
		opts := []kafkaprocessor.Option{
			kafkaprocessor.WithCheckpoint(checkpoint),
			kafkaprocessor.WithLogger(logger),
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, kafkaprocessor.WithInstrumentation(instrumentation))
		}
		kafkaWriter, err := kafkaprocessor.NewBatchWriter(ctx, config.Processor.Kafka.Writer, opts...)
		if err != nil {
			return err
		}
		defer kafkaWriter.Close()
		processor = kafkaWriter
	case config.Processor.Search != nil:
		var searchStore search.Store
		var err error
		searchStore, err = store.NewStore(config.Processor.Search.Store, store.WithLogger(logger))
		if err != nil {
			return err
		}
		searchStore = search.NewStoreRetrier(searchStore, config.Processor.Search.Retrier, search.WithStoreLogger(logger))
		if instrumentation.IsEnabled() {
			searchStore, err = searchinstrumentation.NewStore(searchStore, instrumentation)
			if err != nil {
				return err
			}
		}

		searchIndexer, err := search.NewBatchIndexer(ctx,
			config.Processor.Search.Indexer,
			searchStore,
			pgreplication.NewLSNParser(),
			search.WithCheckpoint(checkpoint),
			search.WithLogger(logger),
		)
		if err != nil {
			return err
		}
		defer searchIndexer.Close()
		processor = searchIndexer

	case config.Processor.Webhook != nil:
		var subscriptionStore webhookstore.Store
		var err error
		subscriptionStore, err = pgwebhook.NewSubscriptionStore(ctx,
			config.Processor.Webhook.SubscriptionStore.URL,
			pgwebhook.WithLogger(logger),
		)
		if err != nil {
			return err
		}

		if config.Processor.Webhook.SubscriptionStore.CacheEnabled {
			logger.Info("setting up subscription store cache...")
			subscriptionStore, err = subscriptionstorecache.New(ctx, subscriptionStore,
				&subscriptionstorecache.Config{
					SyncInterval: config.Processor.Webhook.SubscriptionStore.CacheRefreshInterval,
				},
				subscriptionstorecache.WithLogger(logger))
			if err != nil {
				return err
			}
		}

		notifier := webhooknotifier.New(
			&config.Processor.Webhook.Notifier,
			subscriptionStore,
			webhooknotifier.WithLogger(logger),
			webhooknotifier.WithCheckpoint(checkpoint))
		defer notifier.Close()
		processor = notifier

		subscriptionServer := subscriptionserver.New(
			&config.Processor.Webhook.SubscriptionServer,
			subscriptionStore,
			subscriptionserver.WithLogger(logger))

		eg.Go(func() error {
			defer logger.Info("stopping subscription server...")
			logger.Info("running subscription server...")
			go subscriptionServer.Start()
			<-ctx.Done()
			return subscriptionServer.Shutdown(ctx)
		})
		eg.Go(func() error {
			defer logger.Info("stopping webhook notifier...")
			logger.Info("running webhook notifier...")
			return notifier.Notify(ctx)
		})

	default:
		return errors.New("no processor found")
	}

	if config.Processor.Injector != nil {
		logger.Info("adding injection to processor...")
		opts := []injector.Option{
			injector.WithLogger(logger),
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, injector.WithInstrumentation(instrumentation))
		}
		injector, err := injector.New(config.Processor.Injector, processor, opts...)
		if err != nil {
			return fmt.Errorf("error creating processor injection layer: %w", err)
		}
		defer injector.Close()
		processor = injector
	}

	if processor != nil && instrumentation.IsEnabled() {
		var err error
		processor, err = processinstrumentation.NewProcessor(processor, instrumentation)
		if err != nil {
			return err
		}
	}

	// Listener

	switch {
	case config.Listener.Postgres != nil:
		opts := []pglistener.Option{
			pglistener.WithLogger(logger),
		}
		if config.Listener.Postgres.Snapshot != nil {
			logger.Info("initial snapshot enabled")
			snapshotGenerator, err := pglistener.NewSnapshotGeneratorAdapter(
				ctx,
				config.Listener.Postgres.Snapshot,
				processor.ProcessWALEvent,
				pgsnapshotgenerator.WithLogger(logger))
			if err != nil {
				return err
			}
			defer snapshotGenerator.Close()
			opts = append(opts, pglistener.WithInitialSnapshot(snapshotGenerator))
		}

		var listener listener.Listener = pglistener.New(
			replicationHandler,
			processor.ProcessWALEvent,
			opts...)
		defer listener.Close()

		eg.Go(func() error {
			defer logger.Info("stopping postgres listener...")
			logger.Info("running postgres listener...")
			return listener.Listen(ctx)
		})
	case config.Listener.Kafka != nil:
		var err error
		listener, err := kafkalistener.NewWALReader(
			kafkaReader,
			processor.ProcessWALEvent,
			kafkalistener.WithLogger(logger))
		if err != nil {
			return err
		}
		defer listener.Close()

		eg.Go(func() error {
			defer logger.Info("stopping kafka reader...")
			logger.Info("running kafka reader...")
			return listener.Listen(ctx)
		})
	}

	if err := eg.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	return nil
}
