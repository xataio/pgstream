// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/xataio/pgstream/internal/backoff"
	"github.com/xataio/pgstream/internal/kafka"
	"github.com/xataio/pgstream/pkg/wal"
)

type Checkpointer struct {
	committer       msgCommitter
	backoffProvider backoff.Provider
}

type Config struct {
	Reader        kafka.ReaderConfig
	CommitBackoff backoff.Config
}

type msgCommitter interface {
	CommitMessages(ctx context.Context, msgs ...*kafka.Message) error
	Close() error
}

func New(ctx context.Context, cfg Config) (*Checkpointer, error) {
	reader, err := kafka.NewReader(cfg.Reader)
	if err != nil {
		return nil, err
	}

	return &Checkpointer{
		committer: reader,
		backoffProvider: func(ctx context.Context) backoff.Backoff {
			return backoff.NewExponentialBackoff(ctx, &cfg.CommitBackoff)
		},
	}, nil
}

func (c *Checkpointer) CommitMessages(ctx context.Context, positions []wal.CommitPosition) error {
	msgs := make([]*kafka.Message, 0, len(positions))
	for _, pos := range positions {
		msgs = append(msgs, pos.KafkaPos)
	}

	if err := c.commitMessagesWithRetry(ctx, msgs); err != nil {
		return err
	}

	for _, msg := range msgs {
		log.Trace().
			Str("topic", msg.Topic).
			Int("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Msg("committed")
	}

	return nil
}

func (c *Checkpointer) Close() error {
	return c.committer.Close()
}

func (c *Checkpointer) commitMessagesWithRetry(ctx context.Context, msgs []*kafka.Message) error {
	bo := c.backoffProvider(ctx)
	return bo.RetryNotify(
		func() error {
			return c.committer.CommitMessages(ctx, msgs...)
		},
		func(err error, d time.Duration) {
			log.Warn().Err(err).Msgf("kafka checkpointer: failed to commit messages, retrying in %v", d)
		})
}
