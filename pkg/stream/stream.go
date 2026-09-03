// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/transformers/builder"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	processinstrumentation "github.com/xataio/pgstream/pkg/wal/processor/instrumentation"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/sanitizer"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	searchinstrumentation "github.com/xataio/pgstream/pkg/wal/processor/search/instrumentation"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	stdoutwriter "github.com/xataio/pgstream/pkg/wal/processor/stdout"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	webhooknotifier "github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	subscriptionserver "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	webhookstore "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store"
	subscriptionstorecache "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/cache"
	pgwebhook "github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/postgres"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

type closerFn func() error

type processorType int

const (
	processorTypeReplication processorType = iota
	processorTypeSnapshot
)

func buildProcessor(ctx context.Context, logger loglib.Logger, config *ProcessorConfig, checkpoint checkpointer.Checkpoint, processorType processorType, instrumentation *otel.Instrumentation) (processor.Processor, error) {
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

		searchIndexer, err := search.NewBatchIndexer(
			ctx,
			config.Search.Indexer,
			searchStore,
			pgreplication.NewLSNParser(),
			search.WithCheckpoint(checkpoint),
			search.WithLogger(logger),
			search.WithInstrumentation(instrumentation),
		)
		if err != nil {
			return nil, err
		}
		processor = searchIndexer

	case config.Webhook != nil:
		logger.Info("webhook processor configured")

		var subscriptionStore webhookstore.Store
		var err error
		subscriptionStore, err = pgwebhook.NewSubscriptionStore(
			ctx,
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
			webhooknotifier.WithCheckpoint(checkpoint),
		)
		processor = notifier

		subscriptionServer := subscriptionserver.New(
			&config.Webhook.SubscriptionServer,
			subscriptionStore,
			subscriptionserver.WithLogger(logger),
		)

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

		if processorType == processorTypeSnapshot && config.Postgres.BatchWriter.BulkIngestEnabled {
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
				return nil, fmt.Errorf("target postgres: %w", err)
			}

			processor = pgBatchWriter
		}

	case config.Stdout != nil:
		logger.Info("stdout processor configured")
		processor = stdoutwriter.NewWriter(
			stdoutwriter.WithLogger(logger),
			stdoutwriter.WithCheckpoint(checkpoint),
		)

	default:
		return nil, errors.New("no supported processor found")
	}

	return processor, nil
}

// modifier is a layer wrapped around the target writer. rowVisible reports
// whether it observes or changes row data: such a layer must see every row, so
// a fast path that bypasses the chain is only correct without one.
type modifier struct {
	rowVisible bool
}

var (
	modifierSanitizer       = modifier{rowVisible: true}
	modifierTransformer     = modifier{rowVisible: true}
	modifierInjector        = modifier{rowVisible: true}
	modifierFilter          = modifier{rowVisible: true}
	modifierInstrumentation = modifier{}
)

// processorChain is a target writer with the modifier layers wrapped around
// it. It records which layers were applied, so a fast path that bypasses the
// chain can tell whether bypassing it is safe.
type processorChain struct {
	processor processor.Processor
	applied   []modifier
}

// apply wraps the current processor in a new layer and records it. It is the
// only way to extend the chain: a layer cannot be added without naming a
// modifier, which forces whoever adds one to decide whether it is row
// visible. Recording happens where the wrapping happens, so a layer that is
// configured but not actually applied is not recorded either.
func (c *processorChain) apply(m modifier, wrap func(processor.Processor) (processor.Processor, error)) error {
	p, err := wrap(c.processor)
	if err != nil {
		return err
	}
	c.processor = p
	c.applied = append(c.applied, m)
	return nil
}

// hasRowVisibleLayers reports whether any applied layer must see every row.
// When false, rows may be written straight to the target, bypassing the chain.
func (c *processorChain) hasRowVisibleLayers() bool {
	for _, m := range c.applied {
		if m.rowVisible {
			return true
		}
	}
	return false
}

func addProcessorModifiers(ctx context.Context, config *Config, logger loglib.Logger, target processor.Processor, instrumentation *otel.Instrumentation) (*processorChain, closerFn, error) {
	closerAgg := &closerAggregator{}
	chain := &processorChain{processor: target}

	if config.Processor.Sanitize != nil && config.Processor.Sanitize.StripNullCharBytes {
		logger.Info("adding null byte sanitizer to processor...")
		if err := chain.apply(modifierSanitizer, func(p processor.Processor) (processor.Processor, error) {
			return sanitizer.New(p, sanitizer.WithLogger(logger)), nil
		}); err != nil {
			return nil, nil, err
		}
	}

	if config.Processor.Transformer != nil {
		logger.Info("adding transformation layer to processor...")
		builderOpts := []builder.Option{}
		if instrumentation.IsEnabled() {
			builderOpts = append(builderOpts, builder.WithInstrumentation(instrumentation))
		}
		transformerBuilder := builder.NewTransformerBuilder(builderOpts...)

		opts := []transformer.Option{transformer.WithLogger(logger)}
		// if a source pg url is provided, use it to validate the transformer
		pgURL := config.SourcePostgresURL()
		if pgURL != "" {
			var parser transformer.ParseFn
			parserOpts := []transformer.ParserOption{transformer.WithParserLogger(logger)}
			// only a postgres target enforces the source's unique indexes
			if config.Processor.Postgres != nil {
				parserOpts = append(parserOpts, transformer.WithUniquenessEnforcement())
			}
			pgParser, err := transformer.NewPostgresTransformerParser(ctx, pgURL, transformerBuilder, config.RequiredTables(), parserOpts...)
			if err != nil {
				return nil, nil, fmt.Errorf("creating transformer validator: %w", err)
			}
			closerAgg.addCloserFn(pgParser.Close)
			// warnings only reach the caller here
			parser = func(ctx context.Context, rules transformer.Rules) (*transformer.TransformerMap, error) {
				transformerMap, err := pgParser.ParseAndValidate(ctx, rules)
				for _, warning := range pgParser.Warnings() {
					logger.Warn(nil, warning)
				}
				return transformerMap, err
			}

			// wrap the parser to add inferred rules if enabled. This requires a
			// live connection to the source db and will query the security
			// labels to build the rules. This is only supported for postgres
			// sources
			if config.Processor.Transformer.InferFromSecurityLabels {
				logger.Info("inferring transformation rules from postgres anon security labels...")
				anonRuleParser, err := transformer.NewAnonRuleParser(ctx, pgURL, config.Processor.Transformer.DumpInferredRules, logger, parser)
				if err != nil {
					return nil, nil, fmt.Errorf("creating anon rule parser: %w", err)
				}
				closerAgg.addCloserFn(anonRuleParser.Close)
				parser = anonRuleParser.ParseAndValidate
			}

			opts = append(opts, transformer.WithParser(parser))
		}
		if err := chain.apply(modifierTransformer, func(p processor.Processor) (processor.Processor, error) {
			return transformer.New(ctx, config.Processor.Transformer, p, transformerBuilder, opts...)
		}); err != nil {
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
		if err := chain.apply(modifierInjector, func(p processor.Processor) (processor.Processor, error) {
			return injector.New(ctx, config.Processor.Injector, p, opts...)
		}); err != nil {
			return nil, nil, fmt.Errorf("error creating processor injection layer: %w", err)
		}
	}

	if config.Processor.Filter != nil {
		logger.Info("adding filtering to processor...")
		if err := chain.apply(modifierFilter, func(p processor.Processor) (processor.Processor, error) {
			return filter.New(p, config.Processor.Filter, filter.WithLogger(logger))
		}); err != nil {
			return nil, nil, err
		}
	}

	if chain.processor != nil && instrumentation.IsEnabled() {
		if err := chain.apply(modifierInstrumentation, func(p processor.Processor) (processor.Processor, error) {
			return processinstrumentation.NewProcessor(p, instrumentation)
		}); err != nil {
			return nil, nil, err
		}
	}

	return chain, closerAgg.close, nil
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
