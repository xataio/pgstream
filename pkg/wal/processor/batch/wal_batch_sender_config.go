// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"errors"
	"fmt"
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
	// IgnoreSendErrors indicates whether sending errors should be ignored or
	// not. If set to true, errors will be logged but the batch will continue
	// processing. Defaults to false.
	IgnoreSendErrors bool

	AutoTune AutoTuneConfig
}

type AutoTuneConfig struct {
	// AutoTune indicates whether the batch sender should auto-tune the batch
	// size based on observed throughput. Defaults to false.
	Enabled bool
	// AutoTuneMinBatchBytes is the minimum batch size in bytes when auto-tuning
	// is enabled. Defaults to 1MiB.
	MinBatchBytes int64
	// AutoTuneMaxBatchBytes is the maximum batch size in bytes when auto-tuning
	// is enabled. Defaults to 50MiB.
	MaxBatchBytes int64
	// AutoTuneConvergenceThreshold is the threshold for convergence when
	// auto-tuning is enabled. Defaults to 0.01 (1%).
	ConvergenceThreshold float64
}

const (
	defaultMaxQueueBytes         = int64(100 * 1024 * 1024) // 100MiB
	defaultBatchTimeout          = time.Second
	defaultMaxBatchSize          = 100
	defaultMaxBatchBytes         = int64(1572864)
	defaultAutoTuneMinBatchBytes = int64(1024 * 1024)                         // 1MiB
	defaultAutoTuneMaxBatchBytes = int64(0.5 * float64(defaultMaxQueueBytes)) // 50MiB (50% of max queue bytes)
	defaultConvergenceThreshold  = 0.01                                       // 1%
)

var errInvalidAutoTuneConfig = errors.New("invalid auto tune configuration: min batch bytes must be less than or equal to max batch bytes")

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

func (c *Config) GetAutoTuneConfig() AutoTuneConfig {
	// set default max batch bytes if not set to not exceed max queue bytes
	if c.AutoTune.MaxBatchBytes == 0 {
		maxQueueBytes, _ := c.GetMaxQueueBytes()
		c.AutoTune.MaxBatchBytes = int64(0.5 * float64(maxQueueBytes))
	}
	return c.AutoTune
}

func (c *AutoTuneConfig) IsValid() error {
	if c.GetMinBatchBytes() > c.GetMaxBatchBytes() {
		return fmt.Errorf("min: %d, max: %d: %w", c.GetMinBatchBytes(), c.GetMaxBatchBytes(), errInvalidAutoTuneConfig)
	}
	return nil
}

func (c *AutoTuneConfig) GetMinBatchBytes() int64 {
	if c.MinBatchBytes > 0 {
		return c.MinBatchBytes
	}
	return defaultAutoTuneMinBatchBytes
}

func (c *AutoTuneConfig) GetMaxBatchBytes() int64 {
	if c.MaxBatchBytes > 0 {
		return c.MaxBatchBytes
	}
	return defaultAutoTuneMaxBatchBytes
}

func (c *AutoTuneConfig) GetConvergenceThreshold() float64 {
	if c.ConvergenceThreshold > 0 {
		return c.ConvergenceThreshold
	}
	return defaultConvergenceThreshold
}
