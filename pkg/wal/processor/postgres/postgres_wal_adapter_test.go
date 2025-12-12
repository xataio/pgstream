// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestAdapter_walEventToQueries(t *testing.T) {
	t.Parallel()

	testDDLQuery := &query{
		schema: "public",
		table:  "users",
		sql:    "ALTER TABLE users ADD COLUMN age INT",
		isDDL:  true,
	}

	testDDLAdapter := &mockDDLAdapter{
		schemaLogToQueriesFn: func(ctx context.Context, l *schemalog.LogEntry) ([]*query, error) {
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

	errTest := errors.New("oh noes")

	tests := []struct {
		name            string
		event           *wal.Event
		schemaObserver  schemaObserver
		ddlAdapter      ddlQueryAdapter
		dmlAdapter      dmlQueryAdapter
		logEntryAdapter logEntryAdapter

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
			name: "schema log event with ddl adapter",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "pgstream",
					Table:  "schema_log",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn:         func(schema, table string) bool { return false },
				updateGeneratedColumnNamesFn: func(logEntry *schemalog.LogEntry) {},
				updateMaterializedViewsFn:    func(logEntry *schemalog.LogEntry) {},
			},
			logEntryAdapter: func(data *wal.Data) (*schemalog.LogEntry, error) {
				return &schemalog.LogEntry{}, nil
			},
			dmlAdapter: testDMLAdapter,
			ddlAdapter: testDDLAdapter,

			wantQueries: []*query{testDDLQuery},
			wantErr:     nil,
		},
		{
			name: "schema log event without ddl adapter",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "pgstream",
					Table:  "schema_log",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn:         func(schema, table string) bool { return false },
				updateGeneratedColumnNamesFn: func(logEntry *schemalog.LogEntry) {},
				updateMaterializedViewsFn:    func(logEntry *schemalog.LogEntry) {},
			},
			logEntryAdapter: func(data *wal.Data) (*schemalog.LogEntry, error) {
				return &schemalog.LogEntry{}, nil
			},
			dmlAdapter: testDMLAdapter,
			ddlAdapter: nil,

			wantQueries: []*query{{}},
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
				},
			},
			dmlAdapter: testDMLAdapter,
			ddlAdapter: testDDLAdapter,

			wantQueries: []*query{testDMLQuery},
			wantErr:     nil,
		},
		{
			name: "error - log entry adapter",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: "pgstream",
					Table:  "schema_log",
				},
			},
			schemaObserver: &mockSchemaObserver{
				isMaterializedViewFn:         func(schema, table string) bool { return false },
				updateGeneratedColumnNamesFn: func(logEntry *schemalog.LogEntry) {},
				updateMaterializedViewsFn:    func(logEntry *schemalog.LogEntry) {},
			},
			logEntryAdapter: func(data *wal.Data) (*schemalog.LogEntry, error) {
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
				logEntryAdapter: tc.logEntryAdapter,
			}

			queries, err := a.walEventToQueries(context.Background(), tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.ElementsMatch(t, tc.wantQueries, queries)
		})
	}
}
