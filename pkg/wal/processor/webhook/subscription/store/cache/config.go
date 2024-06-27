// SPDX-License-Identifier: Apache-2.0

package cache

import "time"

type Config struct {
	// SyncInterval represents how frequently the cache will attempt to sync
	// with the internal subscription store to retrieve the latest data. It
	// defaults to 60s.
	SyncInterval time.Duration
}

const (
	defaultSyncInterval = 60 * time.Second
)

func (c *Config) syncInterval() time.Duration {
	if c.SyncInterval > 0 {
		return c.SyncInterval
	}
	return defaultSyncInterval
}
