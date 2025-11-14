// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

func Test_New(t *testing.T) {
	t.Parallel()

	mockProcessor := &mocks.Processor{
		ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
			return nil
		},
	}

	tests := []struct {
		name         string
		config       *Config
		wantIncluded pglib.SchemaTableMap
		wantExcluded pglib.SchemaTableMap
		wantErr      error
	}{
		{
			name: "valid included configuration",
			config: &Config{
				IncludeTables: []string{"users", "public.orders", "public.*", "*.*"},
			},
			wantIncluded: pglib.SchemaTableMap{
				"public": map[string]struct{}{
					"*":       {},
					"orders":  {},
					"users":   {},
					"default": {},
				},
				"*": map[string]struct{}{
					"*": {},
				},
			},
			wantErr: nil,
		},
		{
			name: "valid excluded configuration",
			config: &Config{
				ExcludeTables: []string{"public.users", "public.orders"},
			},
			wantExcluded: pglib.SchemaTableMap{
				"public": map[string]struct{}{
					"orders": {},
					"users":  {},
				},
			},
			wantErr: nil,
		},
		{
			name: "both included and excluded configured",
			config: &Config{
				IncludeTables: []string{"public.users"},
				ExcludeTables: []string{"public.orders"},
			},
			wantErr: errIncludeExcludeList,
		},
		{
			name: "invalid table name in included",
			config: &Config{
				IncludeTables: []string{"invalid.table.name"},
			},
			wantErr: pglib.ErrInvalidTableName,
		},
		{
			name: "invalid table name in excluded",
			config: &Config{
				ExcludeTables: []string{"invalid.table.name"},
			},
			wantErr: pglib.ErrInvalidTableName,
		},
		{
			name:    "empty configuration",
			config:  &Config{},
			wantErr: errMissingFilteringConfig,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			filter, err := New(mockProcessor, tc.config,
				WithDefaultIncludeTables([]string{"default"}),
				WithLogger(log.NewNoopLogger()))
			require.ErrorIs(t, err, tc.wantErr)
			if filter == nil {
				return
			}
			require.Equal(t, tc.wantIncluded, filter.includeTableMap)
			require.Equal(t, tc.wantExcluded, filter.excludeTableMap)
		})
	}
}

func TestFilter_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	nowStr := time.Now().Format("2006-01-02 15:04:05")
	ctx := context.Background()

	testSchema := "test_schema"
	testTable := "test_table"
	testEvent := &wal.Event{
		Data: &wal.Data{
			Schema: testSchema,
			Table:  testTable,
		},
	}
	id := xid.New().String()

	testFullSchema := "{\"tables\": [{\"oid\": \"88374\", \"name\": \"another_test_table\", \"columns\": [{\"name\": \"id\", \"type\": \"integer\", \"unique\": true, \"default\": \"nextval('public.another_test_id_seq'::regclass)\", \"metadata\": null, \"nullable\": false, \"generated\": false, \"pgstream_id\": \"d269f53l60j08uekvnvg-1\"}, {\"name\": \"name\", \"type\": \"text\", \"unique\": false, \"default\": null, \"metadata\": null, \"nullable\": true, \"generated\": false, \"pgstream_id\": \"d269f53l60j08uekvnvg-2\"}, {\"name\": \"age\", \"type\": \"bigint\", \"unique\": false, \"default\": \"0\", \"metadata\": null, \"nullable\": true, \"generated\": false, \"pgstream_id\": \"d269f53l60j08uekvnvg-3\"}], \"pgstream_id\": \"d269f53l60j08uekvnvg\", \"primary_key_columns\": [\"id\"], \"indexes\": null}, {\"oid\": \"109551\", \"name\": \"test_table\", \"columns\": [{\"name\": \"id\", \"type\": \"bigint\", \"unique\": true, \"default\": null, \"metadata\": null, \"nullable\": false, \"generated\": false, \"pgstream_id\": \"d269f53l60j08uekvo00-1\"}, {\"name\": \"name\", \"type\": \"text\", \"unique\": false, \"default\": null, \"metadata\": null, \"nullable\": true, \"generated\": false, \"pgstream_id\": \"d269f53l60j08uekvo00-2\"}, {\"name\": \"username\", \"type\": \"text\", \"unique\": false, \"default\": \"('user_'::text || name)\", \"metadata\": null, \"nullable\": true, \"generated\": true, \"pgstream_id\": \"d269f53l60j08uekvo00-3\"}, {\"name\": \"age\", \"type\": \"integer\", \"unique\": false, \"default\": \"0\", \"metadata\": null, \"nullable\": true, \"generated\": false, \"pgstream_id\": \"d269f53l60j08uekvo00-4\"}], \"pgstream_id\": \"d269f53l60j08uekvo00\", \"primary_key_columns\": [\"id\"], \"indexes\": null}]}"
	testFilteredSchema := "{\"tables\":[{\"oid\":\"109551\",\"name\":\"test_table\",\"columns\":[{\"name\":\"id\",\"type\":\"bigint\",\"nullable\":false,\"generated\":false,\"unique\":true,\"metadata\":null,\"pgstream_id\":\"d269f53l60j08uekvo00-1\"},{\"name\":\"name\",\"type\":\"text\",\"nullable\":true,\"generated\":false,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"d269f53l60j08uekvo00-2\"},{\"name\":\"username\",\"type\":\"text\",\"default\":\"('user_'::text || name)\",\"nullable\":true,\"generated\":true,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"d269f53l60j08uekvo00-3\"},{\"name\":\"age\",\"type\":\"integer\",\"default\":\"0\",\"nullable\":true,\"generated\":false,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"d269f53l60j08uekvo00-4\"}],\"primary_key_columns\":[\"id\"],\"indexes\":null,\"pgstream_id\":\"d269f53l60j08uekvo00\"}]}"

	tests := []struct {
		name      string
		processor *mocks.Processor
		included  pglib.SchemaTableMap
		excluded  pglib.SchemaTableMap
		event     *wal.Event

		wantProcessCalls uint
		wantErr          error
	}{
		{
			name:  "event is nil",
			event: nil,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Nil(t, walEvent)
					return nil
				},
			},
			included: nil,
			excluded: nil,

			wantProcessCalls: 1,
			wantErr:          nil,
		},
		{
			name:  "event data is nil",
			event: &wal.Event{Data: nil},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Nil(t, walEvent.Data)
					return nil
				},
			},
			included: nil,
			excluded: nil,

			wantProcessCalls: 1,
			wantErr:          nil,
		},
		{
			name: "schema log event update not filtered",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: schemalog.SchemaName,
					Table:  schemalog.TableName,
					Action: "U",
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, &wal.Data{
						Schema: schemalog.SchemaName,
						Table:  schemalog.TableName,
						Action: "U",
					}, walEvent.Data)
					return nil
				},
			},
			included: nil,
			excluded: nil,

			wantProcessCalls: 1,
			wantErr:          nil,
		},
		{
			name: "schema log event with included filter",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: schemalog.SchemaName,
					Table:  schemalog.TableName,
					Action: "I",
					Columns: []wal.Column{
						{ID: "id", Name: "id", Type: "text", Value: id},
						{ID: "version", Name: "version", Type: "integer", Value: 0},
						{ID: "schema_name", Name: "schema_name", Type: "text", Value: testSchema},
						{ID: "created_at", Name: "created_at", Type: "timestamp", Value: nowStr},
						{ID: "schema", Name: "schema", Type: "text", Value: testFullSchema},
					},
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.NotNil(t, walEvent.Data)
					wantCols := []wal.Column{
						{ID: "id", Name: "id", Type: "text", Value: id},
						{ID: "version", Name: "version", Type: "integer", Value: 0},
						{ID: "schema_name", Name: "schema_name", Type: "text", Value: testSchema},
						{ID: "created_at", Name: "created_at", Type: "timestamp", Value: nowStr},
						{ID: "schema", Name: "schema", Type: "text", Value: testFilteredSchema},
					}
					require.Equal(t, wantCols, walEvent.Data.Columns)
					return nil
				},
			},
			included: pglib.SchemaTableMap{
				testSchema: {
					testTable: {},
				},
			},
			excluded: nil,

			wantProcessCalls: 1,
			wantErr:          nil,
		},
		{
			name: "schema log event with excluded filter",
			event: &wal.Event{
				Data: &wal.Data{
					Schema: schemalog.SchemaName,
					Table:  schemalog.TableName,
					Action: "I",
					Columns: []wal.Column{
						{ID: "id", Name: "id", Type: "text", Value: id},
						{ID: "version", Name: "version", Type: "integer", Value: 0},
						{ID: "schema_name", Name: "schema_name", Type: "text", Value: testSchema},
						{ID: "created_at", Name: "created_at", Type: "timestamp", Value: nowStr},
						{ID: "schema", Name: "schema", Type: "text", Value: testFullSchema},
					},
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.NotNil(t, walEvent.Data)
					wantCols := []wal.Column{
						{ID: "id", Name: "id", Type: "text", Value: id},
						{ID: "version", Name: "version", Type: "integer", Value: 0},
						{ID: "schema_name", Name: "schema_name", Type: "text", Value: testSchema},
						{ID: "created_at", Name: "created_at", Type: "timestamp", Value: nowStr},
						{ID: "schema", Name: "schema", Type: "text", Value: testFilteredSchema},
					}
					require.Equal(t, wantCols, walEvent.Data.Columns)
					return nil
				},
			},
			included: nil,
			excluded: pglib.SchemaTableMap{
				testSchema: {
					"another_test_table": {},
				},
			},

			wantProcessCalls: 1,
			wantErr:          nil,
		},
		{
			name:  "event matches included",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, testEvent, walEvent)
					return nil
				},
			},
			included: pglib.SchemaTableMap{
				testSchema: {
					testTable: struct{}{},
				},
			},
			excluded: nil,

			wantProcessCalls: 1,
			wantErr:          nil,
		},
		{
			name:  "event matches included *.*",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, testEvent, walEvent)
					return nil
				},
			},
			included: pglib.SchemaTableMap{
				testSchema: {
					"blah": {},
				},
				"*": {
					"*": {},
				},
			},
			excluded: nil,

			wantProcessCalls: 1,
			wantErr:          nil,
		},
		{
			name:  "event matches included *.table",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, testEvent, walEvent)
					return nil
				},
			},
			included: pglib.SchemaTableMap{
				testSchema: {
					"blah": {},
				},
				"*": {
					testTable: {},
				},
			},
			excluded: nil,

			wantProcessCalls: 1,
			wantErr:          nil,
		},
		{
			name:  "event does not match included",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return errors.New("ProcessWALEventFn: should not be called")
				},
			},
			included: pglib.SchemaTableMap{
				"public": {
					"orders": struct{}{},
				},
			},
			excluded: nil,

			wantProcessCalls: 0,
			wantErr:          nil,
		},
		{
			name:  "event matches excluded",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return errors.New("ProcessWALEventFn: should not be called")
				},
			},
			included: nil,
			excluded: pglib.SchemaTableMap{
				testSchema: {
					testTable: struct{}{},
				},
			},

			wantProcessCalls: 0,
			wantErr:          nil,
		},
		{
			name:  "event does not match excluded",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, testEvent, walEvent)
					return nil
				},
			},
			included: nil,
			excluded: pglib.SchemaTableMap{
				"public": {
					"orders": struct{}{},
				},
			},

			wantProcessCalls: 1,
			wantErr:          nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := &Filter{
				logger:          log.NewNoopLogger(),
				processor:       tc.processor,
				excludeTableMap: tc.excluded,
				includeTableMap: tc.included,
			}

			err := f.ProcessWALEvent(ctx, tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantProcessCalls, tc.processor.GetProcessCalls())
		})
	}
}

func TestFilter_filterTablesFromSchema(t *testing.T) {
	t.Parallel()

	makeSchema := func(tables ...string) *schemalog.Schema {
		s := &schemalog.Schema{}
		for _, tbl := range tables {
			s.Tables = append(s.Tables, schemalog.Table{Name: tbl})
		}
		return s
	}

	tests := []struct {
		name        string
		included    pglib.SchemaTableMap
		excluded    pglib.SchemaTableMap
		inputTables []string
		wantTables  []string
	}{
		{
			name:        "no filtering applied",
			included:    nil,
			excluded:    nil,
			inputTables: []string{"users", "orders", "products"},
			wantTables:  []string{"users", "orders", "products"},
		},
		{
			name:        "include only users",
			included:    pglib.SchemaTableMap{"public": {"users": {}}},
			excluded:    nil,
			inputTables: []string{"users", "orders", "products"},
			wantTables:  []string{"users"},
		},
		{
			name:        "include wildcard table",
			included:    pglib.SchemaTableMap{"public": {"*": {}}},
			excluded:    nil,
			inputTables: []string{"users", "orders", "products"},
			wantTables:  []string{"users", "orders", "products"},
		},
		{
			name:        "include wildcard schema and table",
			included:    pglib.SchemaTableMap{"*": {"*": {}}},
			excluded:    nil,
			inputTables: []string{"users", "orders"},
			wantTables:  []string{"users", "orders"},
		},
		{
			name:        "exclude orders",
			included:    nil,
			excluded:    pglib.SchemaTableMap{"public": {"orders": {}}},
			inputTables: []string{"users", "orders", "products"},
			wantTables:  []string{"users", "products"},
		},
		{
			name:        "exclude wildcard table",
			included:    nil,
			excluded:    pglib.SchemaTableMap{"public": {"*": {}}},
			inputTables: []string{"users", "orders", "products"},
			wantTables:  []string{},
		},
		{
			name:        "exclude wildcard schema and table",
			included:    nil,
			excluded:    pglib.SchemaTableMap{"*": {"*": {}}},
			inputTables: []string{"users", "orders"},
			wantTables:  []string{},
		},
		{
			name:        "include users, exclude orders (should only use include)",
			included:    pglib.SchemaTableMap{"public": {"users": {}}},
			excluded:    pglib.SchemaTableMap{"public": {"orders": {}}},
			inputTables: []string{"users", "orders", "products"},
			wantTables:  []string{"users"}, // only include is used
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &Filter{
				includeTableMap: tc.included,
				excludeTableMap: tc.excluded,
			}

			schema := makeSchema(tc.inputTables...)
			f.filterTablesFromSchema("public", schema)

			gotTables := make([]string, len(schema.Tables))
			for i, tbl := range schema.Tables {
				gotTables[i] = tbl.Name
			}
			require.ElementsMatch(t, tc.wantTables, gotTables)
		})
	}
}
