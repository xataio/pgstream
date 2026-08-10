// SPDX-License-Identifier: Apache-2.0

package notifier

import (
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
)

const (
	defaultMaxQueueBytes  = int64(100 * 1024 * 1024)
	defaultURLWorkerCount = 10
	defaultClientTimeout  = 10 * time.Second

	defaultBackoffInitialInterval = time.Second
	defaultBackoffMaxInterval     = 30 * time.Second
	defaultBackoffMaxRetries      = 3
)

type Config struct {
	MaxQueueBytes  int64
	URLWorkerCount uint
	ClientTimeout  time.Duration
	Backoff        backoff.Config
}

func (c *Config) maxQueueBytes() int64 {
	if c.MaxQueueBytes > 0 {
		return c.MaxQueueBytes
	}

	return defaultMaxQueueBytes
}

func (c *Config) workerCount() uint {
	if c.URLWorkerCount > 0 {
		return c.URLWorkerCount
	}

	return defaultURLWorkerCount
}

func (c *Config) clientTimeout() time.Duration {
	if c.ClientTimeout > 0 {
		return c.ClientTimeout
	}

	return defaultClientTimeout
}

func (c *Config) backoffConfig() *backoff.Config {
	// IsSet() is false when only DisableRetries is set.
	if c.Backoff.DisableRetries {
		return &backoff.Config{DisableRetries: true}
	}

	if c.Backoff.Exponential != nil {
		exp := *c.Backoff.Exponential
		// avoids a zero-delay retry storm
		if exp.InitialInterval == 0 && exp.MaxInterval == 0 {
			exp.InitialInterval = defaultBackoffInitialInterval
			exp.MaxInterval = defaultBackoffMaxInterval
		}
		return &backoff.Config{Exponential: &exp}
	}

	if c.Backoff.Constant != nil {
		cst := *c.Backoff.Constant
		if cst.Interval == 0 {
			cst.Interval = defaultBackoffInitialInterval
		}
		return &backoff.Config{Constant: &cst}
	}

	return &backoff.Config{
		Exponential: &backoff.ExponentialConfig{
			InitialInterval: defaultBackoffInitialInterval,
			MaxInterval:     defaultBackoffMaxInterval,
			MaxRetries:      defaultBackoffMaxRetries,
		},
	}
}
