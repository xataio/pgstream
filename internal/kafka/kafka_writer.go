// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	loglib "github.com/xataio/pgstream/pkg/log"
)

// Writer is a wrapper around the kafkago library writer
type Writer struct {
	kafkaWriter *kafka.Writer
}

// Message is a wrapper around the kafkago library message
type Message kafka.Message

type WriterConfig struct {
	Conn          ConnConfig
	BatchTimeout  time.Duration
	BatchBytes    int64
	BatchSize     int
	MaxQueueBytes int64
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

	transport, err := buildTransport(config.Conn.TLS)
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
				NumPartitions:     cfg.Topic.NumPartitions,
				ReplicationFactor: cfg.Topic.ReplicationFactor,
			},
		}

		err := conn.CreateTopics(topicConfigs...)
		if err != nil {
			return fmt.Errorf("creating topic: %w", err)
		}

		return nil
	})
}

func buildTransport(tlsConfig *TLSConfig) (kafka.RoundTripper, error) {
	transport := kafka.DefaultTransport

	if tlsConfig.Enabled {
		tlsConfig, err := newTLSConfig(tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("building TLS config: %w", err)
		}
		transport = &kafka.Transport{TLS: tlsConfig}
	}

	return transport, nil
}
