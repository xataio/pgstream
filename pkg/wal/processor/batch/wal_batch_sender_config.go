// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"errors"
	"time"
)

type Config struct {
	// BatchTime is the max time interval at which the batch sending is
	// triggered. Defaults to 1s
	BatchTimeout time.Duration
	// MaxBatchBytes is the max size in bytes for a given batch. When this size is
	// reached, the batch is sent. Defaults to 1572864 bytes.
	MaxBatchBytes int64
	// MaxBatchSize is the max number of messages to be sent per batch. When this
	// size is reached, the batch is sent. Defaults to 100.
	MaxBatchSize int64
	// MaxQueueBytes is the max memory used by the batch writer for inflight
	// batches. Defaults to 100MiB
	MaxQueueBytes int64
}

const (
	defaultMaxQueueBytes = int64(100 * 1024 * 1024) // 100MiB
	defaultBatchTimeout  = time.Second
	defaultMaxBatchSize  = 100
	defaultMaxBatchBytes = int64(1572864)
)

func (c *Config) GetMaxBatchBytes() int64 {
	if c.MaxBatchBytes > 0 {
		return c.MaxBatchBytes
	}
	return defaultMaxBatchBytes
}

func (c *Config) GetMaxBatchSize() int64 {
	if c.MaxBatchSize > 0 {
		return c.MaxBatchSize
	}
	return defaultMaxBatchSize
}

func (c *Config) GetBatchTimeout() time.Duration {
	if c.BatchTimeout > 0 {
		return c.BatchTimeout
	}
	return defaultBatchTimeout
}

func (c *Config) GetMaxQueueBytes() (int64, error) {
	if c.MaxQueueBytes > 0 {
		if c.MaxQueueBytes < c.GetMaxBatchBytes() {
			return -1, errors.New("max queue bytes must be equal or bigger than max batch bytes")
		}
		return c.MaxQueueBytes, nil
	}

	return defaultMaxQueueBytes, nil
}
