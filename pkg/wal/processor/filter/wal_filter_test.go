// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/log"
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

	ctx := context.Background()

	testSchema := "test_schema"
	testTable := "test_table"
	testEvent := &wal.Event{
		Data: &wal.Data{
			Schema: testSchema,
			Table:  testTable,
		},
	}

	testDDLEvent := &wal.DDLEvent{
		DDL:        "CREATE TABLE test_schema.test_table (col-1 text PRIMARY KEY, col-2 integer);",
		SchemaName: testSchema,
		CommandTag: "CREATE TABLE",
		Objects: []wal.DDLObject{
			{
				Type:       "table",
				Identity:   "test_schema.test_table",
				Schema:     testSchema,
				OID:        "123456",
				PgstreamID: xid.New().String(),
				Columns: []wal.DDLColumn{
					{Attnum: 1, Name: "col-1", Type: "text", Nullable: false, Generated: false, Unique: true},
					{Attnum: 2, Name: "col-2", Type: "integer", Nullable: true, Generated: false, Unique: false},
				},
				PrimaryKeyColumns: []string{"col-1"},
			},
		},
	}

	ddlEventBytes, err := json.Marshal(testDDLEvent)
	require.NoError(t, err)

	errTest := errors.New("oh noes")

	tests := []struct {
		name               string
		processor          *mocks.Processor
		included           pglib.SchemaTableMap
		excluded           pglib.SchemaTableMap
		event              *wal.Event
		walEventToDDLEvent func(*wal.Data) (*wal.DDLEvent, error)

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
			name: "DDL event not filtered",
			event: &wal.Event{
				Data: &wal.Data{
					Action:  wal.LogicalMessageAction,
					Prefix:  wal.DDLPrefix,
					Content: string(ddlEventBytes),
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, &wal.Data{
						Action:  wal.LogicalMessageAction,
						Prefix:  wal.DDLPrefix,
						Content: string(ddlEventBytes),
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
			name: "DDL event filtered via included list",
			event: &wal.Event{
				Data: &wal.Data{
					Action:  wal.LogicalMessageAction,
					Prefix:  wal.DDLPrefix,
					Content: string(ddlEventBytes),
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return nil
				},
			},
			included: pglib.SchemaTableMap{
				testSchema: {
					"another_table": struct{}{},
				},
			},
			excluded: nil,

			wantProcessCalls: 0,
			wantErr:          nil,
		},
		{
			name: "DDL event filtered via excluded list",
			event: &wal.Event{
				Data: &wal.Data{
					Action:  wal.LogicalMessageAction,
					Prefix:  wal.DDLPrefix,
					Content: string(ddlEventBytes),
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return nil
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
			name: "DDL event with parsing error gets processed",
			event: &wal.Event{
				Data: &wal.Data{
					Action:  wal.LogicalMessageAction,
					Prefix:  wal.DDLPrefix,
					Content: string(ddlEventBytes),
				},
			},
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, &wal.Data{
						Action:  wal.LogicalMessageAction,
						Prefix:  wal.DDLPrefix,
						Content: string(ddlEventBytes),
					}, walEvent.Data)
					return nil
				},
			},
			included: nil,
			excluded: nil,
			walEventToDDLEvent: func(data *wal.Data) (*wal.DDLEvent, error) {
				return nil, errTest
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
				logger:             log.NewNoopLogger(),
				processor:          tc.processor,
				excludeTableMap:    tc.excluded,
				includeTableMap:    tc.included,
				walEventToDDLEvent: wal.WalDataToDDLEvent,
			}

			if tc.walEventToDDLEvent != nil {
				f.walEventToDDLEvent = tc.walEventToDDLEvent
			}

			err := f.ProcessWALEvent(ctx, tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantProcessCalls, tc.processor.GetProcessCalls())
		})
	}
}
