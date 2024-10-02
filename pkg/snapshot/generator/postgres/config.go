// SPDX-License-Identifier: Apache-2.0

package postgres

type Config struct {
	// Size of the incremental batches in which the snapshot will be performed.
	// It defaults to 1000 rows per batch.
	BatchSize   uint
	PostgresURL string
}

const defaultBatchSize = 1000

func (c *Config) batchSize() uint {
	if c.BatchSize != 0 {
		return c.BatchSize
	}
	return defaultBatchSize
}
