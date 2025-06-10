// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"strings"
)

type SnapshotConfig struct {
	Tables []string
}

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
