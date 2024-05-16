// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

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

func NewReader(config ReaderConfig) (*Reader, error) {
	logger := log.With().Strs("kafka_servers", config.Conn.Servers).Bool("tls_enabled", config.Conn.TLS.Enabled).Logger() //nolint:forbidigo
	logger.Info().Msg("creating kafka reader")

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

	dialer, err := buildDialer(config.Conn.TLS)
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
			Logger:         makeLogger(log.Trace),
			ErrorLogger:    makeLogger(log.Error),
			StartOffset:    startOffset,
		}),
	}, nil
}

func (r *Reader) ReadMessage(ctx context.Context) (*Message, error) {
	kafkaMsg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	msg := Message(kafkaMsg)
	return &msg, nil
}

func (r *Reader) CommitMessages(ctx context.Context, msgs ...Message) error {
	kafkaMsgs := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		kafkaMsgs = append(kafkaMsgs, kafka.Message(msg))
	}
	return r.reader.CommitMessages(ctx, kafkaMsgs...)
}
