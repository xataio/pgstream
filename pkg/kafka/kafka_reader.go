// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	loglib "github.com/xataio/pgstream/pkg/log"
)

type MessageReader interface {
	FetchMessage(ctx context.Context) (*Message, error)
	CommitOffsets(ctx context.Context, offsets ...*Offset) error
	Close() error
}

// kafkaFetcher is the subset of kafka.Reader methods used by Reader.
// Extracted as an interface to allow testing the first-fetch retry
// logic without a live broker.
type kafkaFetcher interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Reader struct {
	reader     kafkaFetcher
	newReader  func() kafkaFetcher
	logger     loglib.Logger
	firstFetch bool
}

const (
	earliestOffset = "earliest"
	latestOffset   = "latest"

	maxReaderBytes = 25 * 1024 * 1024 // 25 MiB

	// firstFetchTimeout is the maximum time to wait for the first message
	// after joining a consumer group. kafka-go's consumer group reader can
	// get an empty partition assignment when the topic was recently created,
	// and never recovers (segmentio/kafka-go#585, #800, #1314). Recreating
	// the reader forces a fresh group join that picks up the partition.
	firstFetchTimeout  = 5 * time.Second
	firstFetchAttempts = 12
)

func NewReader(config ReaderConfig, logger loglib.Logger) (*Reader, error) {
	logger.Info("creating kafka reader", loglib.Fields{
		"kafka_servers": config.Conn.Servers,
		"tls_enabled":   config.Conn.TLS.Enabled,
	})

	var startOffset int64
	switch config.consumerGroupStartOffset() {
	case "", earliestOffset:
		// default to first offset
		startOffset = kafka.FirstOffset
	case latestOffset:
		startOffset = kafka.LastOffset
	default:
		return nil, fmt.Errorf("unsupported start offset [%s], must be one of [%s, %s]", config.ConsumerGroupStartOffset, earliestOffset, latestOffset)
	}

	dialer, err := buildDialer(&config.Conn.TLS)
	if err != nil {
		return nil, err
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        config.Conn.Servers,
		Topic:          config.Conn.Topic.Name,
		GroupID:        config.consumerGroupID(),
		MaxBytes:       maxReaderBytes, // TODO: this needs to be in sync with the broker max size
		CommitInterval: 0,              // disabled, we call commit ourselves
		Dialer:         dialer,
		Logger:         makeLogger(logger.Trace),
		ErrorLogger:    makeErrLogger(logger.Error),
		StartOffset:    startOffset,
	}

	newReaderFn := func() kafkaFetcher {
		return kafka.NewReader(readerConfig)
	}

	return &Reader{
		reader:     newReaderFn(),
		newReader:  newReaderFn,
		logger:     logger,
		firstFetch: true,
	}, nil
}

// FetchMessage returns the next message from the reader. This call will block
// until a message is available, or an error occurs. It can be stopped by
// canceling the context.
// The message offset needs to be explicitly committed by using CommitMessages.
//
// On the first call, FetchMessage retries with a timeout to work around
// kafka-go consumer group bugs where the reader gets an empty partition
// assignment on newly-created topics and never recovers
// (segmentio/kafka-go#585, #800, #1314).
func (r *Reader) FetchMessage(ctx context.Context) (*Message, error) {
	if r.firstFetch {
		msg, err := r.fetchWithRetry(ctx)
		if err != nil {
			return nil, err
		}
		r.firstFetch = false
		return msg, nil
	}

	kafkaMsg, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	msg := Message(kafkaMsg)
	return &msg, nil
}

// fetchWithRetry attempts to fetch the first message with a timeout, recreating
// the underlying kafka.Reader on each timeout to force a fresh consumer group
// join. This works around kafka-go's failure to recover from empty partition
// assignments on recently-created topics.
func (r *Reader) fetchWithRetry(ctx context.Context) (*Message, error) {
	var lastErr error
	for attempt := range firstFetchAttempts {
		fetchCtx, cancel := context.WithTimeout(ctx, firstFetchTimeout)
		kafkaMsg, err := r.reader.FetchMessage(fetchCtx)
		cancel()

		if err == nil {
			msg := Message(kafkaMsg)
			return &msg, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Only retry on deadline exceeded (the timeout we set). Any other
		// error (network, broker, auth) should propagate immediately.
		if fetchCtx.Err() != context.DeadlineExceeded {
			return nil, fmt.Errorf("reading from kafka: %w", err)
		}

		lastErr = err
		r.logger.Warn(err, "first fetch timed out, recreating reader to force consumer group rejoin", loglib.Fields{
			"attempt":     attempt + 1,
			"max_attempt": firstFetchAttempts,
			"timeout":     firstFetchTimeout,
		})
		if err := r.reader.Close(); err != nil {
			r.logger.Warn(err, "closing kafka reader during retry")
		}
		r.reader = r.newReader()
	}
	return nil, fmt.Errorf("kafka reader: first message not received after %d attempts (%v each): %w", firstFetchAttempts, firstFetchTimeout, lastErr)
}

func (r *Reader) CommitOffsets(ctx context.Context, offsets ...*Offset) error {
	if len(offsets) == 0 {
		return nil
	}

	kafkaMsgs := make([]kafka.Message, 0, len(offsets))
	for _, offset := range offsets {
		kafkaMsgs = append(kafkaMsgs, kafka.Message{
			Topic:     offset.Topic,
			Partition: offset.Partition,
			Offset:    offset.Offset,
		})
	}
	return r.reader.CommitMessages(ctx, kafkaMsgs...)
}

func (r *Reader) Close() error {
	return r.reader.Close()
}
