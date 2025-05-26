// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"testing"

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
		name    string
		walData *wal.Data
		action  onConflictAction

		wantQuery *query
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
					{ID: columnID(2), Name: "name", Value: "alice"},
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
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") VALUES($1, $2)", quotedTestTable),
				args:        []any{1, "alice"},
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
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") VALUES($1, $2) ON CONFLICT DO NOTHING", quotedTestTable),
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
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") VALUES($1, $2) ON CONFLICT (\"id\") DO UPDATE SET \"id\" = EXCLUDED.\"id\", \"name\" = EXCLUDED.\"name\"", quotedTestTable),
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
				sql:         fmt.Sprintf("INSERT INTO %s(\"id\", \"name\") VALUES($1, $2)", quotedTestTable),
				args:        []any{1, "alice"},
			},
		},
		{
			name: "update",
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
			}
			query := a.walDataToQuery(tc.walData)
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

			_, err := newDMLAdapter(tc.action)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
