// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"time"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Config struct {
	URL               string
	MaxConnections    uint
	BatchConfig       batch.Config
	DisableTriggers   bool
	OnConflictAction  string
	BulkIngestEnabled bool
	RetryPolicy       backoff.Config
	IgnoreDDL         bool
	StrictMode        bool
	// IncludeDDLObjectTypes is a list of object type categories for which
	// DDL should be replicated. Only one of IncludeDDLObjectTypes or
	// ExcludeDDLObjectTypes can be set. Ignored if IgnoreDDL is true.
	IncludeDDLObjectTypes []string
	// ExcludeDDLObjectTypes is a list of object type categories for which
	// DDL should be skipped. Only one of IncludeDDLObjectTypes or
	// ExcludeDDLObjectTypes can be set. Ignored if IgnoreDDL is true.
	ExcludeDDLObjectTypes []string
}

const (
	defaultInitialInterval = 500 * time.Millisecond
	defaultMaxInterval     = 30 * time.Second

	// otherwise the observer doubles max_connections
	maxObserverConnections = 16
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

// EffectiveRetryPolicy returns the retry policy once the default is applied.
func (c *Config) EffectiveRetryPolicy() backoff.Config { return c.retryPolicy() }

func (c *Config) poolOptions() []pglib.PoolOption {
	if c.MaxConnections == 0 {
		return nil
	}
	return []pglib.PoolOption{pglib.WithMaxConnections(int32(c.MaxConnections))}
}

func observerConnections(maxConnections int32) int32 {
	return max(1, min(maxConnections, maxObserverConnections))
}
