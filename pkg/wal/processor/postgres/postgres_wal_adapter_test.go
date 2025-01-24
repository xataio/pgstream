// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"testing"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestAdapter_walEventToQuery(t *testing.T) {
	t.Parallel()

	testCommitPosition := wal.CommitPosition("1/0")
	testTable := "table"
	testSchema := "test"

	tests := []struct {
		name  string
		event *wal.Event

		wantQuery *query
		wantErr   error
	}{
		{
			name: "ok - no data",
			event: &wal.Event{
				CommitPosition: testCommitPosition,
			},

			wantQuery: &query{},
			wantErr:   nil,
		},
		{
			name: "ok - schema event",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: schemalog.SchemaName,
					Table:  schemalog.TableName,
				},
				CommitPosition: testCommitPosition,
			},

			wantQuery: &query{},
			wantErr:   nil,
		},
		{
			name: "ok - supported data event",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "T",
					Schema: testSchema,
					Table:  testTable,
				},
				CommitPosition: testCommitPosition,
			},

			wantQuery: &query{
				sql: fmt.Sprintf("TRUNCATE %s", quotedTableName(testSchema, testTable)),
			},
			wantErr: nil,
		},
		{
			name: "ok - unsupported data event",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "X",
					Schema: testSchema,
					Table:  testTable,
				},
				CommitPosition: testCommitPosition,
			},

			wantQuery: &query{},
			wantErr:   nil,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &adapter{}
			query, err := a.walEventToQuery(tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantQuery, query)
		})
	}
}

func TestAdapter_walDataToQuery(t *testing.T) {
	t.Parallel()

	testTableID := xid.New()
	testTable := "table"
	testSchema := "test"
	quotedTestTable := quotedTableName(testSchema, testTable)

	columnID := func(i int) string {
		return fmt.Sprintf("%s-%d", testTableID, i)
	}

	tests := []struct {
		name    string
		walData *wal.Data

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
				sql: fmt.Sprintf("TRUNCATE %s", quotedTestTable),
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
				sql:  fmt.Sprintf("DELETE FROM %s WHERE id = $1", quotedTestTable),
				args: []any{1},
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
				sql:  fmt.Sprintf("DELETE FROM %s WHERE id = $1 AND name = $2", quotedTestTable),
				args: []any{1, "alice"},
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
				sql:  fmt.Sprintf("INSERT INTO %s(id, name) VALUES($1, $2) ON CONFLICT (id) DO NOTHING", quotedTestTable),
				args: []any{1, "alice"},
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
				sql:  fmt.Sprintf("UPDATE %s SET id = $1, name = $2 WHERE id = $3", quotedTestTable),
				args: []any{1, "alice", 1},
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

			wantQuery: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &adapter{}
			query := a.walDataToQuery(tc.walData)
			require.Equal(t, tc.wantQuery, query)
		})
	}
}
