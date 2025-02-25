// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"strings"
)

type SnapshotConfig struct {
	Tables []string
	// SnapshotWorkers represents the number of snapshots the generator will
	// process concurrently. This doesn't affect the parallelism of the tables
	// within each individual snapshot request. It defaults to 1.
	SnapshotWorkers uint
}

const defaultSnapshotWorkers = 1

const publicSchema = "public"

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
