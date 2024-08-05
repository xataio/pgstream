// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"time"

	"github.com/xataio/pgstream/pkg/kafka"
)

type Config struct {
	Kafka kafka.ConnConfig
	// BatchTime is the max time interval at which the batch sending to kafka is
	// triggered. Defaults to 1s
	BatchTimeout time.Duration
	// BatchBytes is the max size in bytes for a given batch. When this size is
	// reached, the batch is sent to kafka. Defaults to 1572864 bytes.
	BatchBytes int64
	// BatchSize is the max number of messages to be sent per batch. When this
	// size is reached, the batch is sent to kafka. Defaults to 100.
	BatchSize int
	// MaxQueueBytes is the max memory used by the batch writer for inflight
	// batches. Defaults to 100MiB
	MaxQueueBytes int64
}

const (
	defaultMaxQueueBytes = int64(100 * 1024 * 1024) // 100MiB
	defaultBatchTimeout  = time.Second
	defaultBatchSize     = 100
	defaultBatchBytes    = int64(1572864)
)

func (c *Config) batchBytes() int64 {
	if c.BatchBytes > 0 {
		return c.BatchBytes
	}
	return defaultBatchBytes
}

func (c *Config) batchSize() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	return defaultBatchSize
}

func (c *Config) batchTimeout() time.Duration {
	if c.BatchTimeout > 0 {
		return c.BatchTimeout
	}
	return defaultBatchTimeout
}

func (c *Config) maxQueueBytes() (int64, error) {
	if c.MaxQueueBytes > 0 {
		if c.MaxQueueBytes < c.batchBytes() {
			return -1, errors.New("max queue bytes must be equal or bigger than batch bytes")
		}
		return c.MaxQueueBytes, nil
	}

	return defaultMaxQueueBytes, nil
}
