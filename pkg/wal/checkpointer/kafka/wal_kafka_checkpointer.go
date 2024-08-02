// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

// Checkpointer is a kafka implementation of the wal checkpointer. It commits
// the message offsets to kafka.
type Checkpointer struct {
	committer       msgCommitter
	backoffProvider backoff.Provider
	logger          loglib.Logger
	offsetParser    kafka.OffsetParser
}

type Config struct {
	Reader        kafka.ReaderConfig
	CommitBackoff backoff.Config
}

type msgCommitter interface {
	CommitOffsets(ctx context.Context, offsets ...*kafka.Offset) error
	Close() error
}

type Option func(c *Checkpointer)

// New returns a kafka checkpointer that commits the message offsets to kafka by
// partition/topic on demand.
func New(ctx context.Context, cfg Config, opts ...Option) (*Checkpointer, error) {
	c := &Checkpointer{
		logger:          loglib.NewNoopLogger(),
		backoffProvider: backoff.NewProvider(&cfg.CommitBackoff),
		offsetParser:    kafka.NewOffsetParser(),
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

func (c *Checkpointer) CommitOffsets(ctx context.Context, positions []wal.CommitPosition) error {
	// keep track of the last offset per topic+partition
	offsetMap := make(map[string]*kafka.Offset, len(positions))
	for _, pos := range positions {
		offset, err := c.offsetParser.FromString(string(pos))
		if err != nil {
			return err
		}

		topicPartition := fmt.Sprintf("%s-%d", offset.Topic, offset.Partition)
		lastOffset, found := offsetMap[topicPartition]
		if !found || lastOffset.Offset < offset.Offset {
			offsetMap[topicPartition] = offset
		}
	}

	offsets := make([]*kafka.Offset, 0, len(offsetMap))
	for _, offset := range offsetMap {
		offsets = append(offsets, offset)
	}

	if err := c.commitOffsetsWithRetry(ctx, offsets); err != nil {
		return err
	}

	for _, offset := range offsets {
		c.logger.Trace("committed", loglib.Fields{
			"topic":     offset.Topic,
			"partition": offset.Partition,
			"offset":    offset.Offset,
		})
	}

	return nil
}

func (c *Checkpointer) Close() error {
	return c.committer.Close()
}

func (c *Checkpointer) commitOffsetsWithRetry(ctx context.Context, offsets []*kafka.Offset) error {
	bo := c.backoffProvider(ctx)
	return bo.RetryNotify(
		func() error {
			return c.committer.CommitOffsets(ctx, offsets...)
		},
		func(err error, d time.Duration) {
			c.logger.Warn(err, fmt.Sprintf("kafka checkpointer: failed to commit offsets, retrying in %v", d))
		})
}
