// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"

	"github.com/xataio/pgstream/pkg/schemalog"
	pglistener "github.com/xataio/pgstream/pkg/wal/listener/postgres"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/translator"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

func (s *Stream) PostgresToKafka(ctx context.Context) error {
	if s.config.KafkaWriter == nil {
		return errors.New("missing kafka writer configuration")
	}

	if s.config.Listener == nil {
		return errors.New("missing postgres listener configuration")
	}

	kafkaWriter, err := kafkaprocessor.NewBatchWriter(*s.config.KafkaWriter)
	if err != nil {
		return err
	}
	defer kafkaWriter.Close()

	// temporary id/version finders until the version requirement is removed,
	// and the id is automatically inferred by the PK.
	idFinder := func(c *schemalog.Column) bool {
		return c.Name == "id"
	}

	versionFinder := func(c *schemalog.Column) bool {
		return c.Name == "version"
	}

	skipSchema := func(string) bool { return false }

	// wrap the kafka writer with a translation step to inject pgstream metadata into the wal events
	translator, err := translator.New(&s.config.Listener.Translator, kafkaWriter, skipSchema, idFinder, versionFinder)
	if err != nil {
		return err
	}
	defer translator.Close()

	listener, err := pglistener.NewListener(ctx, &s.config.Listener.Postgres, translator.ProcessWALEvent)
	if err != nil {
		return err
	}
	defer listener.Close()

	kafkaWriter.SetCheckpoint(listener.Checkpoint)

	eg, ctx := errgroup.WithContext(ctx)
	// go routine to listen to the postgres replication wal
	eg.Go(func() error {
		log.Info().Msg("running postgres listener...")
		return listener.Listen(ctx)
	})

	// go routine to send messages being processed from the wal into kafka
	eg.Go(func() error {
		log.Info().Msg("running kafka batch writer...")
		return kafkaWriter.Send(ctx)
	})

	if err := eg.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	return nil
}
