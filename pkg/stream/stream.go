// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/transformers/builder"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	processinstrumentation "github.com/xataio/pgstream/pkg/wal/processor/instrumentation"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	searchinstrumentation "github.com/xataio/pgstream/pkg/wal/processor/search/instrumentation"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	webhooknotifier "github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	subscriptionserver "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	webhookstore "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store"
	subscriptionstorecache "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/cache"
	pgwebhook "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/postgres"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

type closerFn func() error

func buildProcessor(ctx context.Context, logger loglib.Logger, config *ProcessorConfig, checkpoint checkpointer.Checkpoint, instrumentation *otel.Instrumentation) (processor.Processor, error) {
	var processor processor.Processor
	switch {
	case config.Kafka != nil:
		logger.Info("kafka processor configured")
		opts := []kafkaprocessor.Option{
			kafkaprocessor.WithCheckpoint(checkpoint),
			kafkaprocessor.WithLogger(logger),
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, kafkaprocessor.WithInstrumentation(instrumentation))
		}
		kafkaWriter, err := kafkaprocessor.NewBatchWriter(ctx, config.Kafka.Writer, opts...)
		if err != nil {
			return nil, err
		}
		processor = kafkaWriter
	case config.Search != nil:
		logger.Info("search processor configured")
		var searchStore search.Store
		var err error
		searchStore, err = store.NewStore(config.Search.Store, store.WithLogger(logger))
		if err != nil {
			return nil, err
		}
		searchStore = search.NewStoreRetrier(searchStore, config.Search.Retrier, search.WithStoreLogger(logger))
		if instrumentation.IsEnabled() {
			searchStore, err = searchinstrumentation.NewStore(searchStore, instrumentation)
			if err != nil {
				return nil, err
			}
		}

		searchIndexer, err := search.NewBatchIndexer(ctx,
			config.Search.Indexer,
			searchStore,
			pgreplication.NewLSNParser(),
			search.WithCheckpoint(checkpoint),
			search.WithLogger(logger),
		)
		if err != nil {
			return nil, err
		}
		processor = searchIndexer

	case config.Webhook != nil:
		logger.Info("webhook processor configured")

		var subscriptionStore webhookstore.Store
		var err error
		subscriptionStore, err = pgwebhook.NewSubscriptionStore(ctx,
			config.Webhook.SubscriptionStore.URL,
			pgwebhook.WithLogger(logger),
		)
		if err != nil {
			return nil, err
		}

		if config.Webhook.SubscriptionStore.CacheEnabled {
			logger.Info("setting up subscription store cache...")
			subscriptionStore, err = subscriptionstorecache.New(ctx, subscriptionStore,
				&subscriptionstorecache.Config{
					SyncInterval: config.Webhook.SubscriptionStore.CacheRefreshInterval,
				},
				subscriptionstorecache.WithLogger(logger))
			if err != nil {
				return nil, err
			}
		}

		notifier := webhooknotifier.New(
			&config.Webhook.Notifier,
			subscriptionStore,
			webhooknotifier.WithLogger(logger),
			webhooknotifier.WithCheckpoint(checkpoint))
		processor = notifier

		subscriptionServer := subscriptionserver.New(
			&config.Webhook.SubscriptionServer,
			subscriptionStore,
			subscriptionserver.WithLogger(logger))

		go func() {
			defer logger.Info("stopping subscription server...")
			logger.Info("running subscription server...")
			go subscriptionServer.Start()
			<-ctx.Done()
			if err := subscriptionServer.Shutdown(ctx); err != nil {
				logger.Error(err, "shutting down webhook subscription server")
			}
		}()

		go func() {
			defer logger.Info("stopping webhook notifier...")
			logger.Info("running webhook notifier...")
			if err := notifier.Notify(ctx); err != nil {
				logger.Error(err, "shutting down webhook notifier")
			}
		}()

	case config.Postgres != nil:
		logger.Info("postgres processor configured")

		opts := []pgwriter.WriterOption{
			pgwriter.WithLogger(logger),
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, pgwriter.WithInstrumentation(instrumentation))
		}

		if config.Postgres.BatchWriter.BulkIngestEnabled {
			logger.Info("postgres bulk ingest writer enabled")
			bulkIngestWriter, err := pgwriter.NewBulkIngestWriter(ctx, &config.Postgres.BatchWriter, opts...)
			if err != nil {
				return nil, err
			}
			processor = bulkIngestWriter
		} else {
			opts := append(opts, pgwriter.WithCheckpoint(checkpoint))
			pgBatchWriter, err := pgwriter.NewBatchWriter(ctx, &config.Postgres.BatchWriter, opts...)
			if err != nil {
				return nil, err
			}

			processor = pgBatchWriter
		}

	default:
		return nil, errors.New("no supported processor found")
	}

	return processor, nil
}

func addProcessorModifiers(ctx context.Context, config *Config, logger loglib.Logger, processor processor.Processor, instrumentation *otel.Instrumentation) (processor.Processor, closerFn, error) {
	closerAgg := &closerAggregator{}
	var err error
	if config.Processor.Transformer != nil {
		logger.Info("adding transformation layer to processor...")
		builderOpts := []builder.Option{}
		if instrumentation.IsEnabled() {
			builderOpts = append(builderOpts, builder.WithInstrumentation(instrumentation))
		}
		transformerBuilder := builder.NewTransformerBuilder(builderOpts...)

		opts := []transformer.Option{transformer.WithLogger(logger)}
		// if a source pg url is provided, use it to validate the transformer
		pgURL := ""
		switch {
		case config.Listener.Postgres != nil:
			pgURL = config.Listener.Postgres.Replication.PostgresURL
		case config.Listener.Snapshot != nil:
			pgURL = config.Listener.Snapshot.Generator.URL
		}
		if pgURL != "" {
			pgParser, err := transformer.NewPostgresTransformerParser(ctx, pgURL, transformerBuilder, config.RequiredTables())
			if err != nil {
				return nil, nil, fmt.Errorf("creating transformer validator: %w", err)
			}
			closerAgg.addCloserFn(pgParser.Close)
			opts = append(opts, transformer.WithParser(pgParser.ParseAndValidate))
		}
		processor, err = transformer.New(ctx, config.Processor.Transformer, processor, transformerBuilder, opts...)
		if err != nil {
			logger.Error(err, "creating transformer layer")
			return nil, nil, err
		}
	}

	if config.Processor.Injector != nil {
		logger.Info("adding injection to processor...")
		opts := []injector.Option{
			injector.WithLogger(logger),
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, injector.WithInstrumentation(instrumentation))
		}
		processor, err = injector.New(config.Processor.Injector, processor, opts...)
		if err != nil {
			return nil, nil, fmt.Errorf("error creating processor injection layer: %w", err)
		}
	}

	if config.Processor.Filter != nil {
		logger.Info("adding filtering to processor...")
		var err error
		processor, err = filter.New(processor, config.Processor.Filter,
			filter.WithLogger(logger),
			// by default we include the pgstream schema log table, since we won't
			// be able to replicate DDL changes otherwise. This behaviour can be
			// disabled by adding it to the exclude tables.
			filter.WithDefaultIncludeTables([]string{schemalog.SchemaName + "." + schemalog.TableName}))
		if err != nil {
			return nil, nil, err
		}
	}

	if processor != nil && instrumentation.IsEnabled() {
		var err error
		processor, err = processinstrumentation.NewProcessor(processor, instrumentation)
		if err != nil {
			return nil, nil, err
		}
	}

	return processor, closerAgg.close, nil
}

type closerAggregator struct {
	closers []closerFn
}

func (ca *closerAggregator) addCloserFn(fn closerFn) {
	ca.closers = append(ca.closers, fn)
}

func (ca *closerAggregator) close() error {
	var errs error
	for _, closer := range ca.closers {
		if err := closer(); err != nil {
			if errs != nil {
				errors.Join(errs, err)
				continue
			}
			errs = err
		}
	}
	return errs
}
