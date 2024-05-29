// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/opensearch"
	"golang.org/x/sync/errgroup"
)

func (s *Stream) KafkaToOpensearch(ctx context.Context) error {
	if s.config.Search == nil {
		return errors.New("missing search configuration")
	}
	if s.config.KafkaReader == nil {
		return errors.New("missing kafka reader configuration")
	}

	searchStore, err := opensearch.NewStore(s.config.Search.Store)
	if err != nil {
		return err
	}
	searchIndexer := search.NewBatchIndexer(ctx, s.config.Search.Indexer, searchStore)
	defer searchIndexer.Close()

	kafkaReader, err := kafkalistener.NewReader(*s.config.KafkaReader, searchIndexer.ProcessWALEvent)
	if err != nil {
		return err
	}
	defer kafkaReader.Close()
	searchIndexer.SetCheckpoint(kafkaReader.Checkpoint)

	eg, ctx := errgroup.WithContext(ctx)
	// go routine to listen to the kafka topic with wal replication messages
	eg.Go(func() error {
		log.Info().Msg("running kafka reader...")
		return kafkaReader.Listen(ctx)
	})

	// go routine to send messages being processed from kafka into opensearch
	eg.Go(func() error {
		log.Info().Msg("running search indexer...")
		return searchIndexer.Send(ctx)
	})

	if err := eg.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	return nil
}
