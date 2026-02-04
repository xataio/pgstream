// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestAdapter_walEventToQueries(t *testing.T) {
	t.Parallel()

	testDDLQuery := &query{
		schema: "public",
		table:  "users",
		sql:    "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
		isDDL:  true,
	}

	testDDLAdapter := &mockDDLAdapter{
		walDataToQueriesFn: func(ctx context.Context, d *wal.Data) ([]*query, error) {
			return []*query{testDDLQuery}, nil
		},
	}

	testDMLQuery := &query{
		schema: "public",
		table:  "users",
		sql:    "INSERT INTO users (id, name) VALUES (1, 'Alice')",
		isDDL:  false,
	}

	testDMLAdapter := &mockDMLAdapter{
		walDataToQueriesFn: func(d *wal.Data, schemaInfo schemaInfo) ([]*query, error) {
			return []*query{testDMLQuery}, nil
		},
	}

	testDDLEvent := wal.DDLEvent{
		DDL:        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
		SchemaName: "public",
		Objects: []wal.DDLObject{
			{
				Type:     "table",
				Identity: "public.users",
				Schema:   "public",
				Columns: []wal.DDLColumn{
					{
						Attnum: 1, Name: "id", Type: "integer", Nullable: false, Generated: false, Unique: true,
					},
					{
						Attnum: 2, Name: "name", Type: "text", Nullable: true, Generated: false, Unique: false,
					},
				},
				PrimaryKeyColumns: []string{"id"},
			},
		},
	}

	testDDLEventJSON, err := json.Marshal(testDDLEvent)
	require.NoError(t, err)

	testDDLWalEvent := &wal.Event{
		Data: &wal.Data{
			Action:  wal.LogicalMessageAction,
			Prefix:  wal.DDLPrefix,
			Content: string(testDDLEventJSON),
		},
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name            string
		event           *wal.Event
		schemaObserver  schemaObserver
		ddlAdapter      ddlQueryAdapter
		dmlAdapter      dmlQueryAdapter
		ddlEventAdapter ddlEventAdapter

		wantQueries []*query
		wantErr     error
	}{
		{
			name: "nil event data",
			event: &wal.Event{
				Data: nil,
			},
			schemaObserver: &mockSchemaObserver{},
			dmlAdapter:     testDMLAdapter,
			ddlAdapter:     testDDLAdapter,

			wantQueries: []*query{{}},
			wantErr:     nil,
		},
		{
			name: "materialized view",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "public",
					Table:  "mat_view",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool {
					return schema == "public" && table == "mat_view"
				},
			},
			dmlAdapter: testDMLAdapter,
			ddlAdapter: testDDLAdapter,

			wantQueries: []*query{{}},
			wantErr:     nil,
		},
		{
			name:  "ddl event with ddl adapter",
			event: testDDLWalEvent,

			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				updateFn:             func(ddlEvent *wal.DDLEvent) {},
			},
			ddlEventAdapter: func(d *wal.Data) (*wal.DDLEvent, error) {
				require.Equal(t, testDDLWalEvent.Data, d)
				return &testDDLEvent, nil
			},

			dmlAdapter: testDMLAdapter,
			ddlAdapter: testDDLAdapter,

			wantQueries: []*query{testDDLQuery},
			wantErr:     nil,
		},
		{
			name: "regular dml event",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "public",
					Table:  "users",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				getGeneratedColumnNamesFn: func(ctx context.Context, schema, table string) (map[string]struct{}, error) {
					return map[string]struct{}{}, nil
				},
				getSequenceColumnsFn: func(ctx context.Context, schema, table string) (map[string]string, error) {
					return map[string]string{}, nil
				},
			},
			dmlAdapter: testDMLAdapter,
			ddlAdapter: testDDLAdapter,

			wantQueries: []*query{testDMLQuery},
			wantErr:     nil,
		},
		{
			name:  "error - ddl event adapter",
			event: testDDLWalEvent,
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				updateFn:             func(ddlEvent *wal.DDLEvent) {},
			},
			ddlEventAdapter: func(d *wal.Data) (*wal.DDLEvent, error) {
				return nil, errTest
			},
			dmlAdapter: testDMLAdapter,
			ddlAdapter: testDDLAdapter,

			wantQueries: nil,
			wantErr:     errTest,
		},
		{
			name: "error getting generated columns",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "public",
					Table:  "users",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				getGeneratedColumnNamesFn: func(ctx context.Context, schema, table string) (map[string]struct{}, error) {
					return nil, errTest
				},
				getSequenceColumnsFn: func(ctx context.Context, schema, table string) (map[string]string, error) {
					return nil, errors.New("getSequenceColumns should not be called in this test")
				},
			},
			dmlAdapter: testDMLAdapter,
			ddlAdapter: testDDLAdapter,

			wantQueries: nil,
			wantErr:     errTest,
		},
		{
			name: "error getting sequence columns",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "public",
					Table:  "users",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				getGeneratedColumnNamesFn: func(ctx context.Context, schema, table string) (map[string]struct{}, error) {
					return map[string]struct{}{}, nil
				},
				getSequenceColumnsFn: func(ctx context.Context, schema, table string) (map[string]string, error) {
					return nil, errTest
				},
			},
			dmlAdapter: testDMLAdapter,
			ddlAdapter: testDDLAdapter,

			wantQueries: nil,
			wantErr:     errTest,
		},
		{
			name: "error processing dml event",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "public",
					Table:  "users",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				getGeneratedColumnNamesFn: func(ctx context.Context, schema, table string) (map[string]struct{}, error) {
					return map[string]struct{}{}, nil
				},
				getSequenceColumnsFn: func(ctx context.Context, schema, table string) (map[string]string, error) {
					return map[string]string{}, nil
				},
			},
			dmlAdapter: &mockDMLAdapter{
				walDataToQueriesFn: func(d *wal.Data, schemaInfo schemaInfo) ([]*query, error) {
					return nil, errTest
				},
			},
			ddlAdapter: testDDLAdapter,

			wantQueries: nil,
			wantErr:     errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := adapter{
				dmlAdapter:      tc.dmlAdapter,
				ddlAdapter:      tc.ddlAdapter,
				schemaObserver:  tc.schemaObserver,
				ddlEventAdapter: tc.ddlEventAdapter,
			}

			queries, err := a.walEventToQueries(context.Background(), tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.ElementsMatch(t, tc.wantQueries, queries)
		})
	}
}
