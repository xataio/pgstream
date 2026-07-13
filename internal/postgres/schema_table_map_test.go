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

func TestSchemaTableMap_lookupsDoNotMutateMap(t *testing.T) {
	t.Parallel()

	schemaMap := SchemaTableMap{
		"public": {
			"orders": struct{}{},
		},
		"*": {
			"audit_log": struct{}{},
		},
	}

	// lookups that merge concrete and wildcard schema entries must not leak
	// the wildcard tables into the concrete schema's map, otherwise
	// ContainsExactSchemaTable starts matching wildcard-only entries
	require.True(t, schemaMap.ContainsSchemaTable("public", "audit_log"))
	require.NotNil(t, schemaMap.GetSchemaTables("public"))

	require.Equal(t, SchemaTableMap{
		"public": {
			"orders": struct{}{},
		},
		"*": {
			"audit_log": struct{}{},
		},
	}, schemaMap)
	require.False(t, schemaMap.ContainsExactSchemaTable("public", "audit_log"))
}

func TestSchemaTableMap_GetSchemaTables(t *testing.T) {
	t.Parallel()

	schemaMap := SchemaTableMap{
		"public": {
			"orders": struct{}{},
		},
		"*": {
			"audit_log": struct{}{},
		},
	}

	require.Equal(t, map[string]struct{}{
		"orders":    {},
		"audit_log": {},
	}, schemaMap.GetSchemaTables("public"))
	require.Equal(t, map[string]struct{}{
		"audit_log": {},
	}, schemaMap.GetSchemaTables("private"))
}

func TestSchemaTableMap_ValidateWildcardSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		schemaMap SchemaTableMap
		wantErr   bool
	}{
		{
			name:      "no wildcard schema",
			schemaMap: SchemaTableMap{"public": {"users": struct{}{}}},
		},
		{
			name:      "wildcard schema with wildcard table",
			schemaMap: SchemaTableMap{"*": {"*": struct{}{}}},
		},
		{
			name:      "wildcard schema with specific table",
			schemaMap: SchemaTableMap{"*": {"users": struct{}{}}},
			wantErr:   true,
		},
		{
			name:      "wildcard schema with wildcard and specific tables",
			schemaMap: SchemaTableMap{"*": {"*": struct{}{}, "users": struct{}{}}},
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.schemaMap.ValidateWildcardSchema()
			if tc.wantErr {
				require.ErrorContains(t, err, "wildcard schema must be used with wildcard table")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchemaTableMap_ContainsExactSchemaTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		schema       string
		table        string
		schemaMap    SchemaTableMap
		wantContains bool
	}{
		{
			name:   "exact match",
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
			name:   "wildcard table does not match",
			schema: "public",
			table:  "users",
			schemaMap: SchemaTableMap{
				"public": {
					"*": struct{}{},
				},
			},
			wantContains: false,
		},
		{
			name:   "wildcard schema does not match",
			schema: "public",
			table:  "users",
			schemaMap: SchemaTableMap{
				"*": {
					"users": struct{}{},
				},
			},
			wantContains: false,
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
			name:         "nil SchemaTableMap",
			schema:       "public",
			table:        "users",
			schemaMap:    nil,
			wantContains: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			contains := tc.schemaMap.ContainsExactSchemaTable(tc.schema, tc.table)
			require.Equal(t, tc.wantContains, contains)
		})
	}
}
