// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	loglib "github.com/xataio/pgstream/pkg/log"
	tlslib "github.com/xataio/pgstream/pkg/tls"
)

type MessageWriter interface {
	WriteMessages(context.Context, ...Message) error
	Close() error
}

// Writer is a wrapper around the kafkago library writer
type Writer struct {
	kafkaWriter *kafka.Writer
}

// Message is a wrapper around the kafkago library message
type Message kafka.Message

// Size returns the size of the kafka message value (does not include headers or
// other fields)
func (m Message) Size() int {
	return len(m.Value)
}

func (m Message) IsEmpty() bool {
	return m.Value == nil
}

type WriterConfig struct {
	Conn ConnConfig
	// BatchTimeout is the time limit on how often incomplete message batches
	// will be flushed to kafka. Defaults to 1s.
	BatchTimeout time.Duration
	// BatchBytes limits the maximum size of a request in bytes before being
	// sent to a partition. Defaults to 1048576 bytes.
	BatchBytes int64
	// BatchSize limits how many messages will be buffered before being sent to
	// a partition. Defaults to 100 messages.
	BatchSize int
}

// NewWriter returns a kafka writer that produces messages to the configured
// topic, using the CRC32 hash function to determine which partition to route
// messages to. This ensures that messages with the same key are routed to the
// same partition.
//
// If the topic auto create setting is enabled in the config, it will create it.
func NewWriter(config WriterConfig, logger loglib.Logger) (*Writer, error) {
	logger.Info("creating kafka writer", loglib.Fields{
		"kafka_servers": config.Conn.Servers,
		"tls_enabled":   config.Conn.TLS.Enabled,
	})

	if config.Conn.Topic.AutoCreate {
		if err := createTopic(&config.Conn); err != nil {
			return nil, err
		}
	}

	transport, err := buildTransport(&config.Conn.TLS)
	if err != nil {
		return nil, err
	}

	return &Writer{
		kafkaWriter: &kafka.Writer{
			Addr:         kafka.TCP(config.Conn.Servers...),
			Topic:        config.Conn.Topic.Name,
			RequiredAcks: kafka.RequireAll,
			Balancer:     &kafka.CRC32Balancer{},
			Transport:    transport,
			Logger:       makeLogger(logger.Trace),
			ErrorLogger:  makeErrLogger(logger.Error),
			BatchTimeout: config.BatchTimeout,
			BatchBytes:   config.BatchBytes,
			BatchSize:    config.BatchSize,
		},
	}, nil
}

func (w *Writer) WriteMessages(ctx context.Context, msgs ...Message) error {
	kafkaMsgs := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		kafkaMsgs = append(kafkaMsgs, kafka.Message(msg))
	}
	return w.kafkaWriter.WriteMessages(ctx, kafkaMsgs...)
}

func (w *Writer) Close() error {
	return w.kafkaWriter.Close()
}

func createTopic(cfg *ConnConfig) error {
	return withConnection(cfg, func(conn *kafka.Conn) error {
		topicConfigs := []kafka.TopicConfig{
			{
				Topic:             cfg.Topic.Name,
				NumPartitions:     cfg.Topic.numPartitions(),
				ReplicationFactor: cfg.Topic.replicationFactor(),
			},
		}

		err := conn.CreateTopics(topicConfigs...)
		if err != nil {
			return fmt.Errorf("creating topic: %w", err)
		}

		return nil
	})
}

func buildTransport(cfg *tlslib.Config) (kafka.RoundTripper, error) {
	if cfg.Enabled {
		tlsConfig, err := tlslib.NewConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("building TLS config: %w", err)
		}
		return &kafka.Transport{TLS: tlsConfig}, nil
	}

	return kafka.DefaultTransport, nil
}
