// SPDX-License-Identifier: Apache-2.0

package natsjetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	natslib "github.com/xataio/pgstream/pkg/nats"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

// Checkpointer is a NATS JetStream implementation of the wal checkpointer. It
// commits the message offsets (acks messages) to NATS JetStream.
type Checkpointer struct {
	committer       natsCommitter
	backoffProvider backoff.Provider
	logger          loglib.Logger
	offsetParser    natslib.OffsetParser
}

type Config struct {
	CommitBackoff backoff.Config
}

type natsCommitter interface {
	CommitOffsets(ctx context.Context, offsets ...*natslib.Offset) error
	Close() error
}

type Option func(c *Checkpointer)

// New returns a NATS JetStream checkpointer that commits the message offsets by
// stream/consumer on demand.
func New(ctx context.Context, cfg Config, committer natsCommitter, opts ...Option) (*Checkpointer, error) {
	c := &Checkpointer{
		logger:          loglib.NewNoopLogger(),
		backoffProvider: backoff.NewProvider(&cfg.CommitBackoff),
		offsetParser:    natslib.NewOffsetParser(),
		committer:       committer,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(c *Checkpointer) {
		c.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_nats_jetstream_checkpointer",
		})
	}
}

func (c *Checkpointer) CommitOffsets(ctx context.Context, positions []wal.CommitPosition) error {
	// keep track of the last offset per stream+consumer
	offsetMap := make(map[string]*natslib.Offset, len(positions))
	for _, pos := range positions {
		offset, err := c.offsetParser.FromString(string(pos))
		if err != nil {
			return err
		}

		streamConsumer := fmt.Sprintf("%s-%s", offset.Stream, offset.Consumer)
		lastOffset, found := offsetMap[streamConsumer]
		if !found || lastOffset.StreamSeq < offset.StreamSeq {
			offsetMap[streamConsumer] = offset
		}
	}

	offsets := make([]*natslib.Offset, 0, len(offsetMap))
	for _, offset := range offsetMap {
		offsets = append(offsets, offset)
	}

	if err := c.commitOffsetsWithRetry(ctx, offsets); err != nil {
		return err
	}

	for _, offset := range offsets {
		c.logger.Trace("committed", loglib.Fields{
			"stream":     offset.Stream,
			"consumer":   offset.Consumer,
			"stream_seq": offset.StreamSeq,
		})
	}

	return nil
}

func (c *Checkpointer) Close() error {
	return nil
}

func (c *Checkpointer) commitOffsetsWithRetry(ctx context.Context, offsets []*natslib.Offset) error {
	bo := c.backoffProvider(ctx)
	return bo.RetryNotify(
		func() error {
			return c.committer.CommitOffsets(ctx, offsets...)
		},
		func(err error, d time.Duration) {
			c.logger.Warn(err, fmt.Sprintf("nats jetstream checkpointer: failed to commit offsets, retrying in %v", d))
		})
}
