// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/internal/replication"
	pgreplication "github.com/xataio/pgstream/internal/replication/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	pgcheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/postgres"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	pglistener "github.com/xataio/pgstream/pkg/wal/listener/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/opensearch"
	"github.com/xataio/pgstream/pkg/wal/processor/translator"

	"golang.org/x/sync/errgroup"
)

func Start(ctx context.Context, logger loglib.Logger, config *Config) error {
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
		checkpoint = kafkaCheckpointer.CommitMessages

	case config.Listener.Postgres != nil:
		pgCheckpointer := pgcheckpoint.NewWithHandler(replicationHandler)
		defer pgCheckpointer.Close()
		checkpoint = pgCheckpointer.SyncLSN
	}

	// Processor

	var processor processor.Processor
	switch {
	case config.Processor.Kafka != nil:
		kafkaWriter, err := kafkaprocessor.NewBatchWriter(*config.Processor.Kafka.Writer,
			kafkaprocessor.WithCheckpoint(checkpoint),
			kafkaprocessor.WithLogger(logger))
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
		searchStore, err := opensearch.NewStore(config.Processor.Search.Store, opensearch.WithLogger(logger))
		if err != nil {
			return err
		}
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
