// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"strings"
)

type SnapshotConfig struct {
	Tables         []string
	ExcludedTables []string
}

const publicSchema = "public"

func schemaTableMap(tables []string) map[string][]string {
	schemaTableMap := make(map[string][]string, len(tables))
	for _, table := range tables {
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
