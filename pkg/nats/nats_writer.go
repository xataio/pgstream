// SPDX-License-Identifier: Apache-2.0

package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	loglib "github.com/xataio/pgstream/pkg/log"
)

type MessageWriter interface {
	WriteMessages(context.Context, ...Message) error
	Close() error
}

// Message represents a NATS JetStream message.
type Message struct {
	// Subject is the NATS subject to publish to.
	Subject string
	// Key is the message key used for subject-based routing.
	Key []byte
	// Value is the message payload.
	Value []byte
}

// Size returns the size of the message value (does not include key or other
// fields).
func (m Message) Size() int {
	return len(m.Value)
}

func (m Message) IsEmpty() bool {
	return m.Value == nil
}

// Writer is a NATS JetStream writer that publishes messages to a configured
// stream.
type Writer struct {
	conn    *nats.Conn
	js      jetstream.JetStream
	subject string
}

// NewWriter returns a NATS JetStream writer that produces messages to the
// configured stream subject. If the stream auto create setting is enabled in
// the config, it will create the stream.
func NewWriter(config WriterConfig, logger loglib.Logger) (*Writer, error) {
	logger.Info("creating nats jetstream writer", loglib.Fields{
		"nats_url":    config.Conn.URL,
		"tls_enabled": config.Conn.TLS.Enabled,
	})

	opts, err := buildConnOptions(&config.Conn)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(config.Conn.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("creating jetstream context: %w", err)
	}

	if config.Conn.Stream.AutoCreate {
		if err := createStream(context.Background(), js, &config.Conn.Stream); err != nil {
			nc.Close()
			return nil, err
		}
	}

	subject := defaultSubject(config.Conn.Stream)
	return &Writer{
		conn:    nc,
		js:      js,
		subject: subject,
	}, nil
}

func (w *Writer) WriteMessages(ctx context.Context, msgs ...Message) error {
	for _, msg := range msgs {
		subject := w.subject
		if len(msg.Key) > 0 {
			subject = fmt.Sprintf("%s.%s", w.subject, string(msg.Key))
		}

		if _, err := w.js.Publish(ctx, subject, msg.Value, jetstream.WithMsgID(string(msg.Key))); err != nil {
			return fmt.Errorf("publishing to nats jetstream: %w", err)
		}
	}
	return nil
}

func (w *Writer) Close() error {
	if w.conn != nil {
		w.conn.Close()
	}
	return nil
}

func createStream(ctx context.Context, js jetstream.JetStream, cfg *StreamConfig) error {
	subjects := cfg.Subjects
	if len(subjects) == 0 {
		subjects = []string{cfg.Name + ".>"}
	}

	streamCfg := jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: subjects,
		Replicas: cfg.replicas(),
	}

	if cfg.MaxBytes > 0 {
		streamCfg.MaxBytes = cfg.MaxBytes
	}
	if cfg.MaxAge > 0 {
		streamCfg.MaxAge = cfg.MaxAge
	}

	if _, err := js.CreateOrUpdateStream(ctx, streamCfg); err != nil {
		return fmt.Errorf("creating stream: %w", err)
	}

	return nil
}

// defaultSubject returns the base subject for publishing. If custom subjects
// are configured, the first one is used (without wildcard). Otherwise, the
// stream name is used.
func defaultSubject(cfg StreamConfig) string {
	if len(cfg.Subjects) > 0 {
		return cfg.Name
	}
	return cfg.Name
}
