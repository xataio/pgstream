// SPDX-License-Identifier: Apache-2.0

package postgres

type Config struct {
	// Postgres connection URL. Required.
	URL string
	// BatchPageSize represents the size of the table page range that will be
	// processed concurrently by the table workers. Defaults to 1000.
	BatchPageSize uint
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
}

const (
	defaultBatchPageSize   = 1000
	defaultTableWorkers    = 4
	defaultSchemaWorkers   = 4
	defaultSnapshotWorkers = 1
)

func (c *Config) batchPageSize() uint {
	if c.BatchPageSize > 0 {
		return c.BatchPageSize
	}
	return defaultBatchPageSize
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
