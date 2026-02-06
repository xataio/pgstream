// SPDX-License-Identifier: Apache-2.0

package nats

import (
	"time"

	tlslib "github.com/xataio/pgstream/pkg/tls"
)

type ConnConfig struct {
	URL             string
	Stream          StreamConfig
	TLS             tlslib.Config
	CredentialsFile string
}

type StreamConfig struct {
	// Name is the name of the JetStream stream.
	Name string
	// Subjects is the list of subjects the stream will capture.
	Subjects []string
	// Replicas is the number of stream replicas. Defaults to 1.
	Replicas int
	// AutoCreate defines if the stream should be created if it doesn't exist.
	// Defaults to false.
	AutoCreate bool
	// MaxBytes is the maximum bytes for the stream. Defaults to -1 (unlimited).
	MaxBytes int64
	// MaxAge is the maximum age of messages in the stream. Defaults to 0
	// (unlimited).
	MaxAge time.Duration
}

type WriterConfig struct {
	Conn ConnConfig
}

type ReaderConfig struct {
	Conn ConnConfig
	// ConsumerName is the name of the durable consumer. If not set, defaults
	// to "pgstream-consumer".
	ConsumerName string
	// DeliverPolicy defines where in the stream to start consuming. If not
	// set, defaults to "all".
	DeliverPolicy string
}

const (
	defaultReplicas      = 1
	defaultConsumerName  = "pgstream-consumer"
	defaultDeliverPolicy = deliverAll

	deliverAll  = "all"
	deliverLast = "last"
	deliverNew  = "new"
)

func (c *StreamConfig) replicas() int {
	if c.Replicas > 0 {
		return c.Replicas
	}
	return defaultReplicas
}

func (c *ReaderConfig) consumerName() string {
	if c.ConsumerName != "" {
		return c.ConsumerName
	}
	return defaultConsumerName
}

func (c *ReaderConfig) deliverPolicy() string {
	if c.DeliverPolicy != "" {
		return c.DeliverPolicy
	}
	return defaultDeliverPolicy
}
