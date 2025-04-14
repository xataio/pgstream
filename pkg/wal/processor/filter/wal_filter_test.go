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
		name          string
		config        *Config
		wantWhitelist schemaTableMap
		wantBlacklist schemaTableMap
		wantErr       error
	}{
		{
			name: "valid whitelist configuration",
			config: &Config{
				WhitelistTables: []string{"users", "public.orders", "public.*", "*.*"},
			},
			wantWhitelist: schemaTableMap{
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
			name: "valid blacklist configuration",
			config: &Config{
				BlacklistTables: []string{"public.users", "public.orders"},
			},
			wantBlacklist: schemaTableMap{
				"public": map[string]struct{}{
					"orders": {},
					"users":  {},
				},
			},
			wantErr: nil,
		},
		{
			name: "both whitelist and blacklist configured",
			config: &Config{
				WhitelistTables: []string{"public.users"},
				BlacklistTables: []string{"public.orders"},
			},
			wantErr: errWhitelistBlacklist,
		},
		{
			name: "invalid table name in whitelist",
			config: &Config{
				WhitelistTables: []string{"invalid.table.name"},
			},
			wantErr: errInvalidTableName,
		},
		{
			name: "invalid table name in blacklist",
			config: &Config{
				BlacklistTables: []string{"invalid.table.name"},
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
				WithDefaultWhitelist([]string{"default"}),
				WithLogger(log.NewNoopLogger()))
			require.ErrorIs(t, err, tc.wantErr)
			if filter == nil {
				return
			}
			require.Equal(t, tc.wantWhitelist, filter.tableWhitelist)
			require.Equal(t, tc.wantBlacklist, filter.tableBlacklist)
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
		whitelist schemaTableMap
		blacklist schemaTableMap
		event     *wal.Event

		wantErr error
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
			whitelist: nil,
			blacklist: nil,
			wantErr:   nil,
		},
		{
			name:  "event matches whitelist",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, testEvent, walEvent)
					return nil
				},
			},
			whitelist: schemaTableMap{
				testSchema: {
					testTable: struct{}{},
				},
			},
			blacklist: nil,
			wantErr:   nil,
		},
		{
			name:  "event does not match whitelist",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return errors.New("ProcessWALEventFn: should not be called")
				},
			},
			whitelist: schemaTableMap{
				"public": {
					"orders": struct{}{},
				},
			},
			blacklist: nil,
			wantErr:   nil,
		},
		{
			name:  "event matches blacklist",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					return errors.New("ProcessWALEventFn: should not be called")
				},
			},
			whitelist: nil,
			blacklist: schemaTableMap{
				testSchema: {
					testTable: struct{}{},
				},
			},
			wantErr: nil,
		},
		{
			name:  "event does not match blacklist",
			event: testEvent,
			processor: &mocks.Processor{
				ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
					require.Equal(t, testEvent, walEvent)
					return nil
				},
			},
			whitelist: nil,
			blacklist: schemaTableMap{
				"public": {
					"orders": struct{}{},
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := &Filter{
				logger:         log.NewNoopLogger(),
				processor:      tc.processor,
				tableBlacklist: tc.blacklist,
				tableWhitelist: tc.whitelist,
			}

			err := f.ProcessWALEvent(ctx, tc.event)
			require.ErrorIs(t, err, tc.wantErr)
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
