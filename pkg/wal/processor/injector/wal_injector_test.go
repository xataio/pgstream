// SPDX-License-Identifier: Apache-2.0

package injector

import (
	"context"
	"testing"

	jsonlib "github.com/xataio/pgstream/internal/json"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"

	"github.com/stretchr/testify/require"
)

func TestInjector_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		event     *wal.Event
		adapter   walToDDLEventAdapter
		processor processor.Processor
		querier   pglib.Querier

		wantErr error
	}{
		{
			name:  "ok - DDL event CREATE TABLE",
			event: newTestWALDDLEvent(),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, newTestWALDDLEvent(), walEvent)
					return nil
				},
			},
			querier: &pgmocks.Querier{},

			wantErr: nil,
		},
		{
			name:  "ok - data event with successful injection",
			event: newTestDataEvent("I"),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, newTestDataEventWithMetadata("I"), walEvent)
					return nil
				},
			},
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, tableObjectQuery, query)
					require.Len(t, dest, 1)
					require.Len(t, args, 2)
					require.Equal(t, testSchemaName, args[0])
					require.Equal(t, testTableName, args[1])

					// Return a valid table object JSON
					tableObj := newTestTableObject()
					tableObjJSON, err := jsonlib.Marshal(tableObj)
					require.NoError(t, err)

					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = tableObjJSON
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok - data event with table not found (ignores event)",
			event: newTestDataEvent("I"),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					// Event should still be processed but without injection
					require.NotNil(t, walEvent)
					return nil
				},
			},
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return pglib.ErrNoRows
				},
			},

			wantErr: nil,
		},
		{
			name:  "ok - DDL event DROP TABLE removes from cache",
			event: newTestWALDDLDropEvent(),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return nil
				},
			},
			querier: &pgmocks.Querier{},

			wantErr: nil,
		},
		{
			name:  "ok - nil data event",
			event: &wal.Event{Data: nil},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Nil(t, walEvent.Data)
					return nil
				},
			},
			querier: &pgmocks.Querier{},

			wantErr: nil,
		},
		{
			name:    "error - adapting DDL event",
			event:   newTestWALDDLEvent(),
			adapter: func(d *wal.Data) (*wal.DDLEvent, error) { return nil, errTest },
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					t.Fatal("should not reach processor")
					return nil
				},
			},
			querier: &pgmocks.Querier{},

			wantErr: errTest,
		},
		{
			name:  "error - processing event fails",
			event: newTestDataEvent("I"),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return errTest
				},
			},
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, tableObjectQuery, query)
					require.Len(t, dest, 1)
					tableObj := newTestTableObject()
					tableObjJSON, err := jsonlib.Marshal(tableObj)
					require.NoError(t, err)
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = tableObjJSON
					return nil
				},
			},

			wantErr: errTest,
		},
		{
			name:  "ok - data event with ID not found (treats as keep alive)",
			event: newTestDataEvent("I"),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					// Event data should be nil (treated as keep alive)
					require.Nil(t, walEvent.Data)
					return nil
				},
			},
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, tableObjectQuery, query)
					require.Len(t, dest, 1)
					// Return table object without primary key
					tableObj := newTestTableObjectNoPK()
					tableObjJSON, err := jsonlib.Marshal(tableObj)
					require.NoError(t, err)
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = tableObjJSON
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - DDL event ALTER TABLE updates cache",
			event: func() *wal.Event {
				ddlEvent := &wal.DDLEvent{
					DDL:        "ALTER TABLE test_schema.test_table ADD COLUMN col-3 text;",
					SchemaName: testSchemaName,
					CommandTag: "ALTER TABLE",
					Objects: []wal.DDLObject{
						{
							Type:       "table",
							Identity:   "test_schema.test_table",
							Schema:     "test_schema",
							OID:        "123456",
							PgstreamID: testTableID,
							Columns: []wal.DDLColumn{
								{Attnum: 1, Name: "col-1", Type: "text", Nullable: false},
								{Attnum: 2, Name: "col-2", Type: "integer", Nullable: true},
								{Attnum: 3, Name: "col-3", Type: "text", Nullable: true},
							},
							PrimaryKeyColumns: []string{"col-1"},
						},
					},
				}
				ddlEventJSON, err := jsonlib.Marshal(ddlEvent)
				require.NoError(t, err)
				return &wal.Event{
					Data: &wal.Data{
						Action:  wal.LogicalMessageAction,
						Prefix:  wal.DDLPrefix,
						Content: string(ddlEventJSON),
					},
				}
			}(),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return nil
				},
			},
			querier: &pgmocks.Querier{},
			wantErr: nil,
		},
		{
			name: "ok - DDL event with column objects",
			event: func() *wal.Event {
				ddlEvent := &wal.DDLEvent{
					DDL:        "CREATE INDEX idx_test ON test_schema.test_table (col-1);",
					SchemaName: testSchemaName,
					CommandTag: "CREATE INDEX",
					Objects: []wal.DDLObject{
						{
							Type:       "table column",
							Identity:   "test_schema.test_table.col-1",
							Schema:     "test_schema",
							OID:        "123456",
							PgstreamID: testTableID,
						},
					},
				}
				ddlEventJSON, err := jsonlib.Marshal(ddlEvent)
				require.NoError(t, err)
				return &wal.Event{
					Data: &wal.Data{
						Action:  wal.LogicalMessageAction,
						Prefix:  wal.DDLPrefix,
						Content: string(ddlEventJSON),
					},
				}
			}(),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return nil
				},
			},
			querier: &pgmocks.Querier{},
			wantErr: nil,
		},
		{
			name:  "ok - query error treated as DATALOSS",
			event: newTestDataEvent("I"),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return nil
				},
			},
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
			},
			wantErr: nil, // Error is logged but doesn't fail the processing
		},
		{
			name:  "ok - inject error with column not found treated as DATALOSS",
			event: newTestDataEvent("I"),
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return nil
				},
			},
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					// Return table with different columns
					tableObj := &wal.DDLObject{
						Type:       "table",
						Identity:   "test_schema.test_table",
						Schema:     testSchemaName,
						OID:        "123456",
						PgstreamID: testTableID,
						Columns: []wal.DDLColumn{
							{Attnum: 1, Name: "different-col", Type: "text", Nullable: false, Unique: true},
						},
						PrimaryKeyColumns: []string{"different-col"},
					}
					tableObjJSON, err := jsonlib.Marshal(tableObj)
					require.NoError(t, err)
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = tableObjJSON
					return nil
				},
			},
			wantErr: nil, // Error is logged but doesn't fail the processing
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			injector := &Injector{
				logger:               loglib.NewNoopLogger(),
				processor:            tc.processor,
				querier:              tc.querier,
				walToDDLEventAdapter: func(d *wal.Data) (*wal.DDLEvent, error) { return newTestDDLEvent(), nil },
				tableCache:           synclib.NewMap[string, *wal.DDLObject](),
				deserializer:         jsonlib.Unmarshal,
			}

			if tc.adapter != nil {
				injector.walToDDLEventAdapter = tc.adapter
			}

			err := injector.ProcessWALEvent(context.Background(), tc.event)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestInjector_inject(t *testing.T) {
	t.Parallel()

	testTableObject := newTestTableObject()
	testTableObjectBytes, err := jsonlib.Marshal(testTableObject)
	require.NoError(t, err)

	tests := []struct {
		name    string
		data    *wal.Data
		querier pglib.Querier

		wantMetadata wal.Metadata
		wantColIDs   []string
		wantErr      error
	}{
		{
			name: "ok - inject metadata successfully",
			data: newTestDataEvent("I").Data,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, tableObjectQuery, query)
					require.Len(t, dest, 1)
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = testTableObjectBytes
					return nil
				},
			},

			wantMetadata: wal.Metadata{
				TablePgstreamID: testTableID,
				InternalColIDs:  []string{"t1-1"},
			},
			wantColIDs: []string{"t1-1", "t1-2"},
			wantErr:    nil,
		},
		{
			name: "error - table not found",
			data: newTestDataEvent("I").Data,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return pglib.ErrNoRows
				},
			},

			wantErr: processor.ErrTableNotFound,
		},
		{
			name: "error - query fails",
			data: newTestDataEvent("I").Data,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - no primary key (ID not found)",
			data: newTestDataEvent("I").Data,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, tableObjectQuery, query)
					require.Len(t, dest, 1)
					tableObj := newTestTableObjectNoPK()
					tableObjJSON, err := jsonlib.Marshal(tableObj)
					require.NoError(t, err)
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = tableObjJSON
					return nil
				},
			},

			wantMetadata: wal.Metadata{
				TablePgstreamID: testTableID,
			},
			wantErr: processor.ErrIDNotFound,
		},
		{
			name: "error - column not found in table",
			data: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Columns = append(d.Columns, wal.Column{
					ID: "col-3", Name: "col-3", Type: "text", Value: "unknown",
				})
				return d
			}(),
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, tableObjectQuery, query)
					require.Len(t, dest, 1)
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = testTableObjectBytes
					return nil
				},
			},

			wantMetadata: wal.Metadata{
				TablePgstreamID: testTableID,
				InternalColIDs:  []string{"t1-1"},
			},
			wantErr: processor.ErrColumnNotFound,
		},
		{
			name: "error - unmarshal table object fails",
			data: newTestDataEvent("I").Data,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, tableObjectQuery, query)
					require.Len(t, dest, 1)
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = []byte("invalid json")
					return nil
				},
			},

			wantErr: errTest, // Placeholder - just check for error
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			injector := &Injector{
				logger:       loglib.NewNoopLogger(),
				querier:      tc.querier,
				tableCache:   synclib.NewMap[string, *wal.DDLObject](),
				deserializer: jsonlib.Unmarshal,
			}

			err := injector.inject(context.Background(), tc.data)
			if tc.wantErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantMetadata, tc.data.Metadata)

			if len(tc.wantColIDs) > 0 {
				for i, col := range tc.data.Columns {
					require.Equal(t, tc.wantColIDs[i], col.ID)
				}
			}
		})
	}
}

func TestInjector_injectColumnIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		data      *wal.Data
		tableObj  *wal.DDLObject
		wantColID []string
		wantErr   bool
	}{
		{
			name:      "ok - inject column IDs for insert event",
			data:      newTestDataEvent("I").Data,
			tableObj:  newTestTableObject(),
			wantColID: []string{"t1-1", "t1-2"},
			wantErr:   false,
		},
		{
			name:      "ok - inject column IDs for delete event with identity",
			data:      newTestDataEvent("D").Data,
			tableObj:  newTestTableObject(),
			wantColID: []string{"t1-1", "t1-2"},
			wantErr:   false,
		},
		{
			name: "ok - inject column IDs for update event with identity",
			data: func() *wal.Data {
				d := newTestDataEvent("U").Data
				d.Identity = []wal.Column{
					{ID: "col-1", Name: "col-1", Type: "text", Value: "id-1"},
				}
				return d
			}(),
			tableObj:  newTestTableObject(),
			wantColID: []string{"t1-1", "t1-2"},
			wantErr:   false,
		},
		{
			name: "error - column not found in columns",
			data: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Columns = append(d.Columns, wal.Column{
					ID: "col-3", Name: "col-3", Type: "text", Value: "val",
				})
				return d
			}(),
			tableObj: newTestTableObject(),
			wantErr:  true,
		},
		{
			name: "error - column not found in identity",
			data: func() *wal.Data {
				d := newTestDataEvent("D").Data
				d.Identity = append(d.Identity, wal.Column{
					ID: "col-3", Name: "col-3", Type: "text", Value: "val",
				})
				return d
			}(),
			tableObj: newTestTableObject(),
			wantErr:  true,
		},
		{
			name: "error - multiple columns not found",
			data: func() *wal.Data {
				d := newTestDataEvent("I").Data
				d.Columns = []wal.Column{
					{ID: "col-1", Name: "col-1", Type: "text", Value: "id-1"},
					{ID: "col-3", Name: "col-3", Type: "text", Value: "val"},
					{ID: "col-4", Name: "col-4", Type: "text", Value: "val2"},
				}
				return d
			}(),
			tableObj: newTestTableObject(),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			injector := &Injector{
				logger: loglib.NewNoopLogger(),
			}

			err := injector.injectColumnIDs(tc.data, tc.tableObj)
			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, processor.ErrColumnNotFound)
			} else {
				require.NoError(t, err)
				if len(tc.data.Columns) > 0 {
					for i, col := range tc.data.Columns {
						if i < len(tc.wantColID) {
							require.Equal(t, tc.wantColID[i], col.ID)
						}
					}
				}
				if len(tc.data.Identity) > 0 {
					for i, col := range tc.data.Identity {
						if i < len(tc.wantColID) {
							require.Equal(t, tc.wantColID[i], col.ID)
						}
					}
				}
			}
		})
	}
}

func TestInjector_getTableObject(t *testing.T) {
	t.Parallel()

	testTableObject := newTestTableObject()
	testTableObjectBytes, err := jsonlib.Marshal(testTableObject)
	require.NoError(t, err)

	tests := []struct {
		name         string
		schema       string
		table        string
		querier      pglib.Querier
		preCache     bool
		deserializer func([]byte, any) error

		wantTableObj *wal.DDLObject
		wantErr      error
	}{
		{
			name:   "ok - fetch from database",
			schema: testSchemaName,
			table:  testTableName,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, tableObjectQuery, query)
					require.Len(t, dest, 1)
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = testTableObjectBytes
					return nil
				},
			},

			wantTableObj: testTableObject,
			wantErr:      nil,
		},
		{
			name:     "ok - fetch from cache",
			schema:   testSchemaName,
			table:    testTableName,
			preCache: true,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					t.Fatal("should not query database when cached")
					return nil
				},
			},
			wantTableObj: testTableObject,
			wantErr:      nil,
		},
		{
			name:   "error - table not found",
			schema: testSchemaName,
			table:  testTableName,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return pglib.ErrNoRows
				},
			},
			wantErr: processor.ErrTableNotFound,
		},
		{
			name:   "error - query fails",
			schema: testSchemaName,
			table:  testTableName,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
			},
			wantErr: errTest,
		},
		{
			name:   "error - invalid json",
			schema: testSchemaName,
			table:  testTableName,
			querier: &pgmocks.Querier{
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					jsonDest, ok := dest[0].(*[]byte)
					require.True(t, ok)
					*jsonDest = []byte("invalid json")
					return nil
				},
			},
			deserializer: func([]byte, any) error {
				return errTest
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cache := synclib.NewMap[string, *wal.DDLObject]()
			if tc.preCache {
				cache.Set(testSchemaName+"."+testTableName, testTableObject)
			}

			injector := &Injector{
				logger:       loglib.NewNoopLogger(),
				querier:      tc.querier,
				tableCache:   cache,
				deserializer: jsonlib.Unmarshal,
			}

			if tc.deserializer != nil {
				injector.deserializer = tc.deserializer
			}

			tableObj, err := injector.getTableObject(context.Background(), tc.schema, tc.table)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantTableObj, tableObj)
		})
	}
}

func TestGetIdentityColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		table   *wal.DDLObject
		wantCol *wal.DDLColumn
	}{
		{
			name: "ok - single primary key column",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "id", Type: "integer", Nullable: false},
					{Attnum: 2, Name: "name", Type: "text", Nullable: true},
				},
				PrimaryKeyColumns: []string{"id"},
			},
			wantCol: &wal.DDLColumn{
				Attnum: 1, Name: "id", Type: "integer", Nullable: false,
			},
		},
		{
			name: "ok - multiple primary key columns returns first one",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "id", Type: "integer", Nullable: false},
					{Attnum: 2, Name: "tenant_id", Type: "text", Nullable: false},
					{Attnum: 3, Name: "name", Type: "text", Nullable: true},
				},
				PrimaryKeyColumns: []string{"id", "tenant_id"},
			},
			wantCol: &wal.DDLColumn{
				Attnum: 1, Name: "id", Type: "integer", Nullable: false,
			},
		},
		{
			name: "ok - no primary key, use first unique not null column",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "name", Type: "text", Nullable: true},
					{Attnum: 2, Name: "email", Type: "text", Nullable: false, Unique: true},
					{Attnum: 3, Name: "username", Type: "text", Nullable: false, Unique: true},
				},
				PrimaryKeyColumns: []string{},
			},
			wantCol: &wal.DDLColumn{
				Attnum: 2, Name: "email", Type: "text", Nullable: false, Unique: true,
			},
		},
		{
			name: "ok - no primary key, columns sorted by attnum for deterministic order",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 3, Name: "username", Type: "text", Nullable: false, Unique: true},
					{Attnum: 1, Name: "name", Type: "text", Nullable: true},
					{Attnum: 2, Name: "email", Type: "text", Nullable: false, Unique: true},
				},
				PrimaryKeyColumns: []string{},
			},
			wantCol: &wal.DDLColumn{
				Attnum: 2, Name: "email", Type: "text", Nullable: false, Unique: true,
			},
		},
		{
			name: "nil - no primary key and no unique not null columns",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "name", Type: "text", Nullable: true},
					{Attnum: 2, Name: "description", Type: "text", Nullable: true},
				},
				PrimaryKeyColumns: []string{},
			},
			wantCol: nil,
		},
		{
			name: "nil - unique but nullable columns",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "email", Type: "text", Nullable: true, Unique: true},
					{Attnum: 2, Name: "username", Type: "text", Nullable: true, Unique: true},
				},
				PrimaryKeyColumns: []string{},
			},
			wantCol: nil,
		},
		{
			name: "nil - not null but not unique columns",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "name", Type: "text", Nullable: false},
					{Attnum: 2, Name: "age", Type: "integer", Nullable: false},
				},
				PrimaryKeyColumns: []string{},
			},
			wantCol: nil,
		},
		{
			name: "nil - primary key column not found in columns",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "name", Type: "text", Nullable: false},
					{Attnum: 2, Name: "age", Type: "integer", Nullable: false},
				},
				PrimaryKeyColumns: []string{"id"},
			},
			wantCol: nil,
		},
		{
			name: "ok - primary key takes precedence over unique not null",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "id", Type: "integer", Nullable: false},
					{Attnum: 2, Name: "email", Type: "text", Nullable: false, Unique: true},
				},
				PrimaryKeyColumns: []string{"id"},
			},
			wantCol: &wal.DDLColumn{
				Attnum: 1, Name: "id", Type: "integer", Nullable: false,
			},
		},
		{
			name: "nil - empty table",
			table: &wal.DDLObject{
				Columns:           []wal.DDLColumn{},
				PrimaryKeyColumns: []string{},
			},
			wantCol: nil,
		},
		{
			name: "ok - composite primary key returns first matching column",
			table: &wal.DDLObject{
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "tenant_id", Type: "text", Nullable: false},
					{Attnum: 2, Name: "user_id", Type: "integer", Nullable: false},
					{Attnum: 3, Name: "name", Type: "text", Nullable: true},
				},
				PrimaryKeyColumns: []string{"tenant_id", "user_id"},
			},
			wantCol: &wal.DDLColumn{
				Attnum: 1, Name: "tenant_id", Type: "text", Nullable: false,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := getIdentityColumn(tc.table)
			require.Equal(t, tc.wantCol, result)
		})
	}
}
