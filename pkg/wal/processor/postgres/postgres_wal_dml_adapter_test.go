// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestDMLAdapter_walDataToQuery(t *testing.T) {
	t.Parallel()

	testTableID := xid.New()
	testTable := "table"
	testSchema := "test"
	quotedTestTable := quotedTableName(testSchema, testTable)
	quotedColumnNames := []string{`"id"`, `"name"`}
	columnID := func(i int) string {
		return fmt.Sprintf("%s-%d", testTableID, i)
	}

	tests := []struct {
		name             string
		walData          *wal.Data
		action           onConflictAction
		generatedColumns map[string]struct{}

		wantQuery *query
		wantErr   error
	}{
		{
			name: "truncate",
			walData: &wal.Data{
				Action: "T",
				Schema: testSchema,
				Table:  testTable,
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQuery: &query{
				schema: testSchema,
				table:  testTable,
				sql:    fmt.Sprintf("TRUNCATE %s", quotedTestTable),
			},
		},
		{
			name: "delete with simple primary key",
			walData: &wal.Data{
				Action: "D",
				Schema: testSchema,
				Table:  testTable,
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQuery: &query{
				schema: testSchema,
				table:  testTable,
				sql:    fmt.Sprintf("DELETE FROM %s WHERE \"id\" = $1", quotedTestTable),
				args:   []any{1},
			},
		},
		{
			name: "delete with composite primary key",
			walData: &wal.Data{
				Action: "D",
				Schema: testSchema,
				Table:  testTable,
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1), columnID(2)},
				},
			},

			wantQuery: &query{
				schema: testSchema,
				table:  testTable,
				sql:    fmt.Sprintf("DELETE FROM %s WHERE \"id\" = $1 AND \"name\" = $2", quotedTestTable),
				args:   []any{1, "alice"},
			},
		},
		{
			name: "delete with full identity",
			walData: &wal.Data{
				Action: "D",
				Schema: testSchema,
				Table:  testTable,
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{},
			},

			wantQuery: &query{
				schema: testSchema,
				table:  testTable,
				sql:    fmt.Sprintf("DELETE FROM %s WHERE \"id\" = $1 AND \"name\" = $2", quotedTestTable),
				args:   []any{1, "alice"},
			},
		},
		{
			name: "error - delete",
			walData: &wal.Data{
				Action:   "D",
				Schema:   testSchema,
				Table:    testTable,
				Identity: []wal.Column{},
				Metadata: wal.Metadata{},
			},

			wantQuery: nil,
			wantErr:   errUnableToBuildQuery,
		},
		{
			name: "insert",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQuery: &query{
				schema:      testSchema,
				table:       testTable,
				columnNames: quotedColumnNames,
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
				args:        []any{1, "alice"},
			},
		},
		{
			name: "insert with infinity timestamp",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "created_at", Value: pgtype.Infinity, Type: "timestamptz"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQuery: &query{
				schema:      testSchema,
				table:       testTable,
				columnNames: []string{`"id"`, `"name"`, `"created_at"`},
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\", \"created_at\") OVERRIDING SYSTEM VALUE VALUES($1, $2, $3)", quotedTestTable),
				args:        []any{1, "alice", pgtype.Timestamptz{Valid: true, InfinityModifier: pgtype.Infinity}},
			},
		},
		{
			name: "insert - on conflict do nothing",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			action: onConflictDoNothing,

			wantQuery: &query{
				schema:      testSchema,
				table:       testTable,
				columnNames: quotedColumnNames,
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2) ON CONFLICT DO NOTHING", quotedTestTable),
				args:        []any{1, "alice"},
			},
		},
		{
			name: "insert - on conflict do update",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},
			action: onConflictUpdate,

			wantQuery: &query{
				schema:      testSchema,
				table:       testTable,
				columnNames: quotedColumnNames,
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2) ON CONFLICT (\"id\") DO UPDATE SET \"id\" = EXCLUDED.\"id\", \"name\" = EXCLUDED.\"name\"", quotedTestTable),
				args:        []any{1, "alice"},
			},
		},
		{
			name: "insert - on conflict do update without PK",
			walData: &wal.Data{
				Action: "I",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
			},
			action: onConflictUpdate,

			wantQuery: &query{
				schema:      testSchema,
				table:       testTable,
				columnNames: quotedColumnNames,
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES($1, $2)", quotedTestTable),
				args:        []any{1, "alice"},
			},
		},
		{
			name: "update - primary key",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQuery: &query{
				schema: testSchema,
				table:  testTable,
				sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3", quotedTestTable),
				args:   []any{1, "alice", 1},
			},
		},
		{
			name: "update - default identity",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
				},
				Metadata: wal.Metadata{},
			},

			wantQuery: &query{
				schema: testSchema,
				table:  testTable,
				sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3", quotedTestTable),
				args:   []any{1, "alice", 1},
			},
		},
		{
			name: "update - full identity",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "a"},
				},
				Metadata: wal.Metadata{},
			},

			wantQuery: &query{
				schema: testSchema,
				table:  testTable,
				sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3 AND \"name\" = $4", quotedTestTable),
				args:   []any{1, "alice", 1, "a"},
			},
		},
		{
			name: "update - with generated column",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
					{ID: columnID(3), Name: "generated_col", Value: "gen_value"},
				},
				Identity: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
				},
				Metadata: wal.Metadata{},
			},
			generatedColumns: map[string]struct{}{`"generated_col"`: {}},

			wantQuery: &query{
				schema: testSchema,
				table:  testTable,
				sql:    fmt.Sprintf("UPDATE %s SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3", quotedTestTable),
				args:   []any{1, "alice", 1},
			},
		},
		{
			name: "error - update",
			walData: &wal.Data{
				Action: "U",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Identity: []wal.Column{},
				Metadata: wal.Metadata{},
			},

			wantQuery: nil,
			wantErr:   errUnableToBuildQuery,
		},
		{
			name: "unknown",
			walData: &wal.Data{
				Action: "X",
				Schema: testSchema,
				Table:  testTable,
				Columns: []wal.Column{
					{ID: columnID(1), Name: "id", Value: 1},
					{ID: columnID(2), Name: "name", Value: "alice"},
				},
				Metadata: wal.Metadata{
					InternalColIDs: []string{columnID(1)},
				},
			},

			wantQuery: &query{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &dmlAdapter{
				onConflictAction: tc.action,
				forCopy:          true,
			}
			queries, err := a.walDataToQueries(tc.walData, schemaInfo{
				generatedColumns: tc.generatedColumns,
			})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantQuery, query)
		})
	}
}

func Test_newDMLAdapter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action string

		wantErr error
	}{
		{
			action:  "update",
			wantErr: nil,
		},
		{
			action:  "nothing",
			wantErr: nil,
		},
		{
			action:  "error",
			wantErr: nil,
		},
		{
			action:  "",
			wantErr: nil,
		},
		{
			action:  "invalid",
			wantErr: errUnsupportedOnConflictAction,
		},
	}

	for _, tc := range tests {
		t.Run(tc.action, func(t *testing.T) {
			t.Parallel()

			_, err := newDMLAdapter(tc.action, false)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestDMLAdapter_filterRowColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		generatedColumns map[string]struct{}
		columns          []wal.Column

		wantColumns []string
		wantValues  []any
	}{
		{
			name:             "no generated columns",
			generatedColumns: map[string]struct{}{},
			columns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
			},

			wantColumns: []string{`"id"`, `"name"`},
			wantValues:  []any{1, "alice"},
		},
		{
			name:             "with generated column",
			generatedColumns: map[string]struct{}{"id": {}},
			columns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
				{Name: "age", Value: 30},
			},

			wantColumns: []string{`"name"`, `"age"`},
			wantValues:  []any{"alice", 30},
		},
		{
			name:             "unknown generated columns",
			generatedColumns: map[string]struct{}{"age": {}},
			columns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
			},

			wantColumns: []string{`"id"`, `"name"`},
			wantValues:  []any{1, "alice"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := dmlAdapter{}
			rowColumns, rowValues := a.filterRowColumns(tc.columns, schemaInfo{
				generatedColumns: tc.generatedColumns,
			})
			require.Equal(t, tc.wantColumns, rowColumns)
			require.Equal(t, tc.wantValues, rowValues)
		})
	}
}
