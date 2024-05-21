// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/xataio/pgstream/internal/backoff"
	"github.com/xataio/pgstream/internal/kafka"
	"github.com/xataio/pgstream/pkg/wal"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	// if we go over this limit the log will likely be truncated and it will not
	// be very readable
	logMaxBytes = 10000
)

type Reader struct {
	reader kafkaReader

	// processRecord is called for a new record.
	processRecord payloadProcessor

	backoffProvider backoff.Provider
}

type ReaderConfig struct {
	Kafka         kafka.ReaderConfig
	CommitBackoff backoff.Config
}

type kafkaReader interface {
	FetchMessage(context.Context) (*kafka.Message, error)
	CommitMessages(context.Context, ...*kafka.Message) error
	Close() error
}

type payloadProcessor func(context.Context, []byte, wal.CommitPosition) error

func NewReader(config ReaderConfig,
	processRecord payloadProcessor,
) (*Reader, error) {
	reader, err := kafka.NewReader(config.Kafka)
	if err != nil {
		return nil, err
	}

	return &Reader{
		reader:        reader,
		processRecord: processRecord,
		backoffProvider: func(ctx context.Context) backoff.Backoff {
			return backoff.NewExponentialBackoff(ctx, &config.CommitBackoff)
		},
	}, nil
}

func (r *Reader) Listen(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := r.reader.FetchMessage(ctx)
			if err != nil {
				return fmt.Errorf("reading from kafka: %w", err)
			}

			log.Ctx(ctx).Trace().
				Str("topic", msg.Topic).
				Int("partition", msg.Partition).
				Int64("offset", msg.Offset).
				Bytes("key", msg.Key).
				Bytes("wal_data", msg.Value).
				Msg("received")

			if err = r.processRecord(ctx, msg.Value, wal.CommitPosition{KafkaPos: msg}); err != nil {
				if errors.Is(err, context.Canceled) {
					return fmt.Errorf("canceled: %w", err)
				}

				logEvent := log.Ctx(ctx).Error().
					Str("severity", "DATALOSS").
					Err(err)
				addBytesToLog(logEvent, "wal_data", msg.Value).Msg("processing kafka msg")
			}
		}
	}
}

func (r *Reader) Close() {
	// Cleanly closing the connection to Kafka is important
	// in order for the consumer's partitions to be re-allocated
	// quickly.
	if err := r.reader.Close(); err != nil {
		log.WithLevel(zerolog.ErrorLevel).
			Err(err).
			Bytes("stack_trace", debug.Stack()).
			Msg("error closing connection to kafka")
	}
}

func (r *Reader) checkpoint(ctx context.Context, positions []wal.CommitPosition) error {
	msgs := make([]*kafka.Message, 0, len(positions))
	for _, pos := range positions {
		msgs = append(msgs, pos.KafkaPos)
	}

	if err := r.commitMessagesWithRetry(ctx, msgs); err != nil {
		return err
	}

	for _, msg := range msgs {
		log.Ctx(ctx).Trace().
			Str("topic", msg.Topic).
			Int("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Msg("committed")
	}

	return nil
}

func (r *Reader) commitMessagesWithRetry(ctx context.Context, msgs []*kafka.Message) error {
	bo := r.backoffProvider(ctx)
	return bo.RetryNotify(
		func() error {
			return r.reader.CommitMessages(ctx, msgs...)
		},
		func(err error, d time.Duration) {
			log.Ctx(ctx).Warn().Err(err).Msgf("failed to commit messages. Retrying in %v", d)
		})
}

func addBytesToLog(log *zerolog.Event, key string, value []byte) *zerolog.Event {
	if len(value) > logMaxBytes {
		return log.Bytes(key, value[:logMaxBytes])
	}
	return log.Bytes(key, value)
}
