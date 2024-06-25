// SPDX-License-Identifier: Apache-2.0

package notifier

import "time"

type Config struct {
	// MaxQueueBytes is the max memory used by the webhook notifier for inflight
	// events. Defaults to 100MiB
	MaxQueueBytes int64
	// URLWorkerCount is the max number of concurrent workers that will send
	// webhooks for a given event. Defaults to 10.
	URLWorkerCount uint
	// ClientTimeout is the max time the notifier will wait for a response from
	// a webhook url before it times out. Defaults to 10s.
	ClientTimeout time.Duration
}

const (
	defaultMaxQueueBytes  = int64(100 * 1024 * 1024) // 100MiB
	defaultURLWorkerCount = 10
	defaultClientTimeout  = 10 * time.Second
)

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
