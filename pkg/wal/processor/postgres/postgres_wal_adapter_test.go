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

func TestAdapter_walEventToMessage(t *testing.T) {
	t.Parallel()

	testDDLEvent := wal.DDLEvent{
		DDL:        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
		SchemaName: "public",
		Objects: []wal.DDLObject{
			{
				Type:     "table",
				Identity: "public.users",
				Schema:   "public",
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

	testGeneratedCols := map[string]struct{}{"gen": {}}
	testSeqCols := map[string]string{"id": "users_id_seq"}
	testEnumCols := map[string]struct{}{`"mood"`: {}}

	tests := []struct {
		name            string
		event           *wal.Event
		schemaObserver  schemaObserver
		ddlAdapter      ddlQueryAdapter
		ddlEventAdapter ddlEventAdapter

		wantMsg *walMessage
		wantErr error
	}{
		{
			name: "nil event data",
			event: &wal.Event{
				Data: nil,
			},
			schemaObserver: &mockSchemaObserver{},

			wantMsg: &walMessage{},
			wantErr: nil,
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

			wantMsg: &walMessage{},
			wantErr: nil,
		},
		{
			name:  "ddl event with ddl adapter",
			event: testDDLWalEvent,
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				updateFn:             func(ddlEvent *wal.DDLEvent) {},
			},
			ddlEventAdapter: func(d *wal.Data) (*wal.DDLEvent, error) {
				return &testDDLEvent, nil
			},
			ddlAdapter: &mockDDLAdapter{},

			wantMsg: &walMessage{data: testDDLWalEvent.Data, isDDL: true},
			wantErr: nil,
		},
		{
			name:  "ddl event without ddl adapter",
			event: testDDLWalEvent,
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				updateFn:             func(ddlEvent *wal.DDLEvent) {},
			},
			ddlEventAdapter: func(d *wal.Data) (*wal.DDLEvent, error) {
				return &testDDLEvent, nil
			},
			ddlAdapter: nil,

			wantMsg: &walMessage{},
			wantErr: nil,
		},
		{
			name: "regular dml event",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "public",
					Table:  "users",
					Action: "I",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				getSchemaInfoFn: func(ctx context.Context, schema, table string) (schemaInfo, error) {
					return schemaInfo{
						generatedColumns: testGeneratedCols,
						sequenceColumns:  testSeqCols,
						enumColumns:      testEnumCols,
					}, nil
				},
			},

			wantMsg: &walMessage{
				data: &wal.Data{Schema: "public", Table: "users", Action: "I"},
				schemaInfo: schemaInfo{
					generatedColumns: testGeneratedCols,
					sequenceColumns:  testSeqCols,
					enumColumns:      testEnumCols,
				},
			},
			wantErr: nil,
		},
		{
			name:  "error - ddl event adapter",
			event: testDDLWalEvent,
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
			},
			ddlEventAdapter: func(d *wal.Data) (*wal.DDLEvent, error) {
				return nil, errTest
			},

			wantMsg: nil,
			wantErr: errTest,
		},
		{
			name: "error getting schema info",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "public",
					Table:  "users",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				getSchemaInfoFn: func(ctx context.Context, schema, table string) (schemaInfo, error) {
					return schemaInfo{}, errTest
				},
			},

			wantMsg: nil,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := adapter{
				ddlAdapter:      tc.ddlAdapter,
				schemaObserver:  tc.schemaObserver,
				ddlEventAdapter: tc.ddlEventAdapter,
			}

			msg, err := a.walEventToMessage(context.Background(), tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantMsg, msg)
		})
	}
}

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
			name: "error getting schema info",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "public",
					Table:  "users",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn: func(schema, table string) bool { return false },
				getSchemaInfoFn: func(ctx context.Context, schema, table string) (schemaInfo, error) {
					return schemaInfo{}, errTest
				},
			},
			dmlAdapter: &mockDMLAdapter{
				walDataToQueriesFn: func(d *wal.Data, si schemaInfo) ([]*query, error) {
					return nil, errors.New("dml adapter should not be called when schema info fails")
				},
			},
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

// TestAdapter_threadsSchemaInfo pins that whatever the observer reports reaches
// the dml adapter (and the wal message) unaltered. Without it the enum columns
// could be dropped from the wiring and no test would fail.
func TestAdapter_threadsSchemaInfo(t *testing.T) {
	t.Parallel()

	wantInfo := schemaInfo{
		generatedColumns:      map[string]struct{}{`"gen"`: {}},
		alwaysIdentityColumns: map[string]struct{}{`"id"`: {}},
		sequenceColumns:       map[string]string{`"id"`: `"public"."users_id_seq"`},
		enumColumns:           map[string]struct{}{`"mood"`: {}},
	}

	event := &wal.Event{Data: &wal.Data{Schema: "public", Table: "users", Action: "I"}}
	observer := &mockSchemaObserver{
		isMaterializedViewFn: func(schema, table string) bool { return false },
		getSchemaInfoFn: func(ctx context.Context, schema, table string) (schemaInfo, error) {
			return wantInfo, nil
		},
	}

	var gotInfo schemaInfo
	a := adapter{
		schemaObserver: observer,
		dmlAdapter: &mockDMLAdapter{
			walDataToQueriesFn: func(d *wal.Data, si schemaInfo) ([]*query, error) {
				gotInfo = si
				return []*query{{sql: "INSERT"}}, nil
			},
		},
	}

	_, err := a.walEventToQueries(context.Background(), event)
	require.NoError(t, err)
	require.Equal(t, wantInfo, gotInfo)

	msg, err := a.walEventToMessage(context.Background(), event)
	require.NoError(t, err)
	require.Equal(t, wantInfo, msg.schemaInfo)
}
