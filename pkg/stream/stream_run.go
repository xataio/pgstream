// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	pgcheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/postgres"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	pglistener "github.com/xataio/pgstream/pkg/wal/listener/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor"
	processinstrumentation "github.com/xataio/pgstream/pkg/wal/processor/instrumentation"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/opensearch"
	"github.com/xataio/pgstream/pkg/wal/processor/translator"
	webhooknotifier "github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	subscriptionserver "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	webhookstore "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store"
	subscriptionstorecache "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/cache"
	pgwebhook "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/postgres"
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationinstrumentation "github.com/xataio/pgstream/pkg/wal/replication/instrumentation"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"

	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
)

// Run will run the configured pgstream processes. This call is blocking.
func Run(ctx context.Context, logger loglib.Logger, config *Config, meter metric.Meter) error {
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
			return fmt.Errorf("error setting up postgres replication handler")
		}
		defer replicationHandler.Close()
	}

	if replicationHandler != nil && meter != nil {
		var err error
		replicationHandler, err = replicationinstrumentation.NewHandler(replicationHandler, meter)
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
		if meter != nil {
			opts = append(opts, kafkaprocessor.WithInstrumentation(meter))
		}
		kafkaWriter, err := kafkaprocessor.NewBatchWriter(config.Processor.Kafka.Writer, opts...)
		if err != nil {
			return err
		}
		defer kafkaWriter.Close()
		processor = kafkaWriter

		// the kafka batch writer requires to initialise a go routine to send
		// the batches asynchronously
		eg.Go(func() error {
			logger.Info("running kafka batch writer...")
			return kafkaWriter.Send(ctx)
		})
	case config.Processor.Search != nil:
		var searchStore search.Store
		var err error
		searchStore, err = opensearch.NewStore(config.Processor.Search.Store, opensearch.WithLogger(logger))
		if err != nil {
			return err
		}
		searchStore = search.NewStoreRetrier(searchStore, config.Processor.Search.Retrier, search.WithStoreLogger(logger))

		searchIndexer := search.NewBatchIndexer(ctx,
			config.Processor.Search.Indexer,
			searchStore,
			pgreplication.NewLSNParser(),
			search.WithCheckpoint(checkpoint),
			search.WithLogger(logger),
		)
		defer searchIndexer.Close()
		processor = searchIndexer

		// the search batch indexer requires to initialise a go routine to send
		// the batches asynchronously
		eg.Go(func() error {
			logger.Info("running search batch indexer...")
			return searchIndexer.Send(ctx)
		})

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
			logger.Info("running subscription server...")
			go subscriptionServer.Start()
			<-ctx.Done()
			return subscriptionServer.Shutdown(ctx)
		})
		eg.Go(func() error {
			logger.Info("running webhook notifier...")
			return notifier.Notify(ctx)
		})

	default:
		return errors.New("no processor found")
	}

	if config.Processor.Translator != nil {
		logger.Info("adding translation to processor...")
		translator, err := translator.New(config.Processor.Translator, processor, translator.WithLogger(logger))
		if err != nil {
			return fmt.Errorf("error creating processor translation layer: %w", err)
		}
		defer translator.Close()
		processor = translator
	}

	if processor != nil && meter != nil {
		var err error
		processor, err = processinstrumentation.NewProcessor(processor, meter)
		if err != nil {
			return err
		}
	}

	// Listener

	switch {
	case config.Listener.Postgres != nil:
		listener := pglistener.New(replicationHandler,
			processor.ProcessWALEvent,
			pglistener.WithLogger(logger))
		defer listener.Close()

		eg.Go(func() error {
			logger.Info("running postgres listener...")
			return listener.Listen(ctx)
		})
	case config.Listener.Kafka != nil:
		var err error
		listener, err := kafkalistener.NewReader(config.Listener.Kafka.Reader,
			processor.ProcessWALEvent,
			kafkalistener.WithLogger(logger))
		if err != nil {
			return err
		}
		defer listener.Close()

		eg.Go(func() error {
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
