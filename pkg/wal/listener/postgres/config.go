// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"

	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/data/postgres"
)

type SnapshotConfig struct {
	Generator        pgsnapshotgenerator.Config
	SnapshotStoreURL string
	Tables           []string
	SchemaLogStore   schemalogpg.Config
	// SnapshotWorkers represents the number of snapshots the generator will
	// process concurrently. This doesn't affect the parallelism of the tables
	// within each individual snapshot request. It defaults to 1.
	SnapshotWorkers uint
}

const defaultSnapshotWorkers = 1

func (c *SnapshotConfig) schemaTableMap() map[string][]string {
	schemaTableMap := make(map[string][]string, len(c.Tables))
	for _, table := range c.Tables {
		schemaName := publicSchema
		tableName := table
		tableSplit := strings.Split(table, ".")
		if len(tableSplit) == 2 {
			schemaName = tableSplit[0]
			tableName = tableSplit[1]
		}
		schemaTableMap[schemaName] = append(schemaTableMap[schemaName], tableName)
	}
	return schemaTableMap
}

func (c *SnapshotConfig) snapshotWorkers() uint {
	if c.SnapshotWorkers > 0 {
		return c.SnapshotWorkers
	}
	return defaultSnapshotWorkers
}
