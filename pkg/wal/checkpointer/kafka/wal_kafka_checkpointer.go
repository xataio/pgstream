// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/internal/backoff"
	"github.com/xataio/pgstream/internal/kafka"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

type Checkpointer struct {
	committer       msgCommitter
	backoffProvider backoff.Provider
	logger          loglib.Logger
}

type Config struct {
	Reader        kafka.ReaderConfig
	CommitBackoff backoff.Config
}

type msgCommitter interface {
	CommitMessages(ctx context.Context, msgs ...*kafka.Message) error
	Close() error
}

type Option func(c *Checkpointer)

func New(ctx context.Context, cfg Config, opts ...Option) (*Checkpointer, error) {
	c := &Checkpointer{
		logger: loglib.NewNoopLogger(),
		backoffProvider: func(ctx context.Context) backoff.Backoff {
			return backoff.NewExponentialBackoff(ctx, &cfg.CommitBackoff)
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	var err error
	c.committer, err = kafka.NewReader(cfg.Reader, c.logger)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(c *Checkpointer) {
		c.logger = loglib.NewLogger(l)
	}
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
		c.logger.Trace("committed", loglib.Fields{
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"offset":    msg.Offset,
		})
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
			c.logger.Warn(err, fmt.Sprintf("kafka checkpointer: failed to commit messages, retrying in %v", d))
		})
}
