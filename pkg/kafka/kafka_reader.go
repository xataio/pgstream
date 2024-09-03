// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	loglib "github.com/xataio/pgstream/pkg/log"
)

type MessageReader interface {
	FetchMessage(ctx context.Context) (*Message, error)
	CommitOffsets(ctx context.Context, offsets ...*Offset) error
	Close() error
}

type Reader struct {
	reader *kafka.Reader
}

type ReaderConfig struct {
	Conn                     ConnConfig
	ConsumerGroupID          string
	ConsumerGroupStartOffset string
}

const (
	earliestOffset = "earliest"
	latestOffset   = "latest"

	maxReaderBytes = 25 * 1024 * 1024 // 25 MiB
)

func NewReader(config ReaderConfig, logger loglib.Logger) (*Reader, error) {
	logger.Info("creating kafka reader", loglib.Fields{
		"kafka_servers": config.Conn.Servers,
		"tls_enabled":   config.Conn.TLS.Enabled,
	})

	var startOffset int64
	switch config.ConsumerGroupStartOffset {
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

	return &Reader{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        config.Conn.Servers,
			Topic:          config.Conn.Topic.Name,
			GroupID:        config.ConsumerGroupID,
			MaxBytes:       maxReaderBytes, // TODO: this needs to be in sync with the broker max size
			CommitInterval: 0,              // disabled, we call commit ourselves
			Dialer:         dialer,
			Logger:         makeLogger(logger.Trace),
			ErrorLogger:    makeErrLogger(logger.Error),
			StartOffset:    startOffset,
		}),
	}, nil
}

// FetchMessage returns the next message from the reader. This call will block
// until a message is available, or an error occurs. It can be stopped by
// canceling the context.
// The message offset needs to be explicitly committed by using CommitMessages.
func (r *Reader) FetchMessage(ctx context.Context) (*Message, error) {
	kafkaMsg, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}

	msg := Message(kafkaMsg)
	return &msg, nil
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
