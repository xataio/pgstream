// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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
		wantIncluded schemaTableMap
		wantExcluded schemaTableMap
		wantErr      error
	}{
		{
			name: "valid included configuration",
			config: &Config{
				IncludeTables: []string{"users", "public.orders", "public.*", "*.*"},
			},
			wantIncluded: schemaTableMap{
				"public": map[string]struct{}{
					"*":       {},
					"orders":  {},
					"users":   {},
					"default": {},
				},
				wildcard: map[string]struct{}{
					wildcard: {},
				},
			},
			wantErr: nil,
		},
		{
			name: "valid excluded configuration",
			config: &Config{
				ExcludeTables: []string{"public.users", "public.orders"},
			},
			wantExcluded: schemaTableMap{
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
			wantErr: errInvalidTableName,
		},
		{
			name: "invalid table name in excluded",
			config: &Config{
				ExcludeTables: []string{"invalid.table.name"},
			},
			wantErr: errInvalidTableName,
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

	tests := []struct {
		name      string
		processor *mocks.Processor
		included  schemaTableMap
		excluded  schemaTableMap
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
			name:  "event matches included",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, testEvent, walEvent)
					return nil
				},
			},
			included: schemaTableMap{
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
			included: schemaTableMap{
				testSchema: {
					"blah": {},
				},
				wildcard: {
					wildcard: {},
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
			included: schemaTableMap{
				testSchema: {
					"blah": {},
				},
				wildcard: {
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
			included: schemaTableMap{
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
			excluded: schemaTableMap{
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
			excluded: schemaTableMap{
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

func TestSchemaTableMap_containsSchemaTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		schema       string
		table        string
		schemaMap    schemaTableMap
		wantContains bool
	}{
		{
			name:   "table exists in schema",
			schema: "public",
			table:  "users",
			schemaMap: schemaTableMap{
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
			schemaMap: schemaTableMap{
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
			schemaMap: schemaTableMap{
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
			schemaMap: schemaTableMap{
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
			schemaMap: schemaTableMap{
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
			schemaMap: schemaTableMap{
				"*": {
					"*": struct{}{},
				},
			},
			wantContains: true,
		},
		{
			name:         "empty schemaTableMap",
			schema:       "public",
			table:        "users",
			schemaMap:    schemaTableMap{},
			wantContains: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			contains := tc.schemaMap.containsSchemaTable(tc.schema, tc.table)
			require.Equal(t, tc.wantContains, contains)
		})
	}
}
