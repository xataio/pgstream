// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Config struct {
	URL               string
	BatchConfig       batch.Config
	DisableTriggers   bool
	OnConflictAction  string
	BulkIngestEnabled bool
	RetryPolicy       backoff.Config
	IgnoreDDL         bool
	// StrictMode stops processing on non-internal query failures instead of
	// dropping them and continuing. When nil, it defaults to disabled for
	// replication and enabled for snapshots.
	StrictMode *bool
}

func (c *Config) strictModeEnabled() bool {
	return c.StrictMode != nil && *c.StrictMode
}

const (
	defaultInitialInterval = 500 * time.Millisecond
	defaultMaxInterval     = 30 * time.Second
)

func (c *Config) retryPolicy() backoff.Config {
	if c.RetryPolicy.IsSet() {
		return c.RetryPolicy
	}
	return backoff.Config{
		Exponential: &backoff.ExponentialConfig{
			InitialInterval: defaultInitialInterval,
			MaxInterval:     defaultMaxInterval,
		},
	}
}
