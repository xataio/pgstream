// SPDX-License-Identifier: Apache-2.0

package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	loglib "github.com/xataio/pgstream/pkg/log"
)

type MessageReader interface {
	FetchMessage(ctx context.Context) (*Message, error)
	CommitOffsets(ctx context.Context, offsets ...*Offset) error
	Close() error
}

// Reader is a NATS JetStream reader that consumes messages from a stream using
// a durable pull consumer.
type Reader struct {
	conn     *nats.Conn
	consumer jetstream.Consumer
	msgs     jetstream.MessagesContext
	// pendingAcks tracks messages by stream sequence for acknowledgment.
	pendingAcks map[uint64]jetstream.Msg
}

// NewReader returns a NATS JetStream reader that consumes messages from the
// configured stream using a durable pull consumer.
func NewReader(config ReaderConfig, logger loglib.Logger) (*Reader, error) {
	logger.Info("creating nats jetstream reader", loglib.Fields{
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

	consumerCfg := jetstream.ConsumerConfig{
		Durable:   config.consumerName(),
		AckPolicy: jetstream.AckExplicitPolicy,
	}

	switch config.deliverPolicy() {
	case deliverAll, "":
		consumerCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case deliverLast:
		consumerCfg.DeliverPolicy = jetstream.DeliverLastPolicy
	case deliverNew:
		consumerCfg.DeliverPolicy = jetstream.DeliverNewPolicy
	default:
		nc.Close()
		return nil, fmt.Errorf("unsupported deliver policy [%s], must be one of [%s, %s, %s]",
			config.DeliverPolicy, deliverAll, deliverLast, deliverNew)
	}

	consumer, err := js.CreateOrUpdateConsumer(context.Background(), config.Conn.Stream.Name, consumerCfg)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("creating consumer: %w", err)
	}

	msgs, err := consumer.Messages()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("creating message iterator: %w", err)
	}

	return &Reader{
		conn:        nc,
		consumer:    consumer,
		msgs:        msgs,
		pendingAcks: make(map[uint64]jetstream.Msg),
	}, nil
}

// FetchMessage returns the next message from the reader. This call will block
// until a message is available, or the context is cancelled. The message offset
// needs to be explicitly committed by using CommitOffsets.
func (r *Reader) FetchMessage(ctx context.Context) (*Message, error) {
	msg, err := r.msgs.Next()
	if err != nil {
		return nil, err
	}

	metadata, err := msg.Metadata()
	if err != nil {
		return nil, fmt.Errorf("getting message metadata: %w", err)
	}

	// Track message for later ack
	r.pendingAcks[metadata.Sequence.Stream] = msg

	return &Message{
		Subject: msg.Subject(),
		Value:   msg.Data(),
	}, nil
}

func (r *Reader) CommitOffsets(ctx context.Context, offsets ...*Offset) error {
	for _, offset := range offsets {
		msg, ok := r.pendingAcks[offset.StreamSeq]
		if !ok {
			continue
		}
		if err := msg.Ack(); err != nil {
			return fmt.Errorf("acking message at stream seq %d: %w", offset.StreamSeq, err)
		}
		delete(r.pendingAcks, offset.StreamSeq)
	}
	return nil
}

func (r *Reader) Close() error {
	if r.msgs != nil {
		r.msgs.Stop()
	}
	if r.conn != nil {
		r.conn.Close()
	}
	return nil
}
