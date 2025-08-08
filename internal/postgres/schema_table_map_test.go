// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaTableMap_containsSchemaTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		schema       string
		table        string
		schemaMap    SchemaTableMap
		wantContains bool
	}{
		{
			name:   "table exists in schema",
			schema: "public",
			table:  "users",
			schemaMap: SchemaTableMap{
				"public": {
					"users": struct{}{},
				},
			},
			wantContains: true,
		},
		{
			name:   "table does not exist in schema",
			schema: "public",
			table:  "orders",
			schemaMap: SchemaTableMap{
				"public": {
					"users": struct{}{},
				},
			},
			wantContains: false,
		},
		{
			name:   "wildcard matches any table in schema",
			schema: "public",
			table:  "orders",
			schemaMap: SchemaTableMap{
				"public": {
					"*": struct{}{},
				},
			},
			wantContains: true,
		},
		{
			name:   "schema does not exist",
			schema: "private",
			table:  "users",
			schemaMap: SchemaTableMap{
				"public": {
					"users": struct{}{},
				},
			},
			wantContains: false,
		},
		{
			name:   "wildcard schema matches any schema",
			schema: "private",
			table:  "users",
			schemaMap: SchemaTableMap{
				"*": {
					"users": struct{}{},
				},
			},
			wantContains: true,
		},
		{
			name:   "wildcard schema and table match",
			schema: "private",
			table:  "orders",
			schemaMap: SchemaTableMap{
				"*": {
					"*": struct{}{},
				},
			},
			wantContains: true,
		},
		{
			name:         "empty SchemaTableMap",
			schema:       "public",
			table:        "users",
			schemaMap:    SchemaTableMap{},
			wantContains: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			contains := tc.schemaMap.ContainsSchemaTable(tc.schema, tc.table)
			require.Equal(t, tc.wantContains, contains)
		})
	}
}
