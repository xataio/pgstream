// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/backoff"
)

type Config struct {
	// Postgres connection URL. Required.
	URL string
	// BatchBytes represents the size of the batch of table pages in bytes.
	// Defaults to 80MiB.
	BatchBytes uint64
	// SnapshotWorkers represents the number of snapshots the generator will
	// process concurrently. This doesn't affect the parallelism of the tables
	// within each individual snapshot request. It defaults to 1.
	SnapshotWorkers uint
	// SchemaWorkers represents the number of tables the snapshot generator will
	// process concurrently per schema. Defaults to 4.
	SchemaWorkers uint
	// TableWorkers represents the number of concurrent workers per table. Each
	// worker will process a different page range in parallel. Defaults to 4.
	TableWorkers uint
	// MaxConnections represents the maximum number of connections that the
	// snapshot generator can open to Postgres. This setting is optional.
	// Defaults to 50
	MaxConnections uint
	// RawJSONValues makes the snapshot read json/jsonb column values as their
	// raw text representation (Go string) instead of unmarshalling them into
	// Go values. Unmarshalling is lossy: the JSON null value ('null'::jsonb)
	// becomes Go nil, indistinguishable from SQL NULL, so it would get written
	// to postgres targets as SQL NULL. Off by default since other targets
	// expect unmarshalled values. This setting is derived from the stream
	// configuration for postgres targets, not set by users.
	RawJSONValues bool
	// derived from stream config
	CopyPassthrough *CopyPassthroughConfig
}

// the generator writes the target
type CopyPassthroughConfig struct {
	TargetURL       string
	DisableTriggers bool
	// 0 defers to the url
	MaxConnections uint
	RetryPolicy    backoff.Config
}

const (
	defaultTableWorkers    = 4
	defaultSchemaWorkers   = 4
	defaultSnapshotWorkers = 1
	defaultBatchBytes      = 80 * 1024 * 1024 // 80 MiB
	defaultMaxConnections  = 50
)

// array_recv ignores user-defined OID mismatch
// needs target schema from source
const copyFormat = " WITH (FORMAT binary)"

func (c *CopyPassthroughConfig) poolOptions() []pglib.PoolOption {
	if c.MaxConnections == 0 {
		return nil
	}
	return []pglib.PoolOption{pglib.WithMaxConnections(int32(c.MaxConnections))}
}

func (c *Config) batchBytes() uint64 {
	if c.BatchBytes > 0 {
		return c.BatchBytes
	}
	return defaultBatchBytes
}

func (c *Config) schemaWorkers() uint {
	if c.SchemaWorkers > 0 {
		return c.SchemaWorkers
	}
	return defaultSchemaWorkers
}

func (c *Config) tableWorkers() uint {
	if c.TableWorkers > 0 {
		return c.TableWorkers
	}
	return defaultTableWorkers
}

func (c *Config) snapshotWorkers() uint {
	if c.SnapshotWorkers > 0 {
		return c.SnapshotWorkers
	}
	return defaultSnapshotWorkers
}

// EffectiveSnapshotWorkers returns the number of snapshots processed
// concurrently once the default is applied.
func (c *Config) EffectiveSnapshotWorkers() uint { return c.snapshotWorkers() }

// EffectiveTableWorkers returns the number of concurrent workers per table once
// the default is applied.
func (c *Config) EffectiveTableWorkers() uint { return c.tableWorkers() }

func (c *Config) maxConnections() uint {
	if c.MaxConnections > 0 {
		return c.MaxConnections
	}
	return defaultMaxConnections
}
