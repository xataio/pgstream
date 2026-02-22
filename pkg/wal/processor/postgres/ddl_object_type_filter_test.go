// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestNewDDLObjectTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		include []string
		exclude []string

		wantNil bool
		wantErr bool
	}{
		{
			name:    "no filter configured",
			wantNil: true,
		},
		{
			name:    "include only",
			include: []string{"tables", "sequences", "types"},
		},
		{
			name:    "exclude only",
			exclude: []string{"functions", "views"},
		},
		{
			name:    "both set - error",
			include: []string{"tables"},
			exclude: []string{"functions"},
			wantErr: true,
		},
		{
			name:    "unknown include category",
			include: []string{"unknown_category"},
			wantErr: true,
		},
		{
			name:    "unknown exclude category",
			exclude: []string{"unknown_category"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f, err := newDDLObjectTypeFilter(tc.include, tc.exclude)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tc.wantNil {
				require.Nil(t, f)
				return
			}

			require.NotNil(t, f)
		})
	}
}

func TestDDLObjectTypeFilter_ShouldSkipDDL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		include  []string
		exclude  []string
		ddlEvent *wal.DDLEvent

		wantSkip bool
	}{
		{
			name: "nil filter - no skip",
			ddlEvent: &wal.DDLEvent{
				Objects: []wal.DDLObject{{Type: "function"}},
			},
			wantSkip: false,
		},
		{
			name:     "nil ddl event - no skip",
			exclude:  []string{"functions"},
			wantSkip: false,
		},
		{
			name:    "exclude functions - function object skipped",
			exclude: []string{"functions"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE FUNCTION",
				Objects:    []wal.DDLObject{{Type: "function", Identity: "public.my_func", Schema: "public"}},
			},
			wantSkip: true,
		},
		{
			name:    "exclude functions - aggregate object skipped",
			exclude: []string{"functions"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE AGGREGATE",
				Objects:    []wal.DDLObject{{Type: "aggregate", Identity: "public.my_agg", Schema: "public"}},
			},
			wantSkip: true,
		},
		{
			name:    "exclude functions - table object not skipped",
			exclude: []string{"functions"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE TABLE",
				Objects:    []wal.DDLObject{{Type: "table", Identity: "public.users", Schema: "public"}},
			},
			wantSkip: false,
		},
		{
			name:    "include tables only - function skipped",
			include: []string{"tables", "sequences"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE FUNCTION",
				Objects:    []wal.DDLObject{{Type: "function", Identity: "public.my_func", Schema: "public"}},
			},
			wantSkip: true,
		},
		{
			name:    "include tables only - table not skipped",
			include: []string{"tables", "sequences"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE TABLE",
				Objects:    []wal.DDLObject{{Type: "table", Identity: "public.users", Schema: "public"}},
			},
			wantSkip: false,
		},
		{
			name:    "include tables - mixed objects with table and index - not skipped",
			include: []string{"tables", "sequences"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE TABLE",
				Objects: []wal.DDLObject{
					{Type: "table", Identity: "public.users", Schema: "public"},
					{Type: "index", Identity: "public.users_pkey", Schema: "public"},
				},
			},
			wantSkip: false,
		},
		{
			name:    "include tables - all objects excluded - skipped",
			include: []string{"tables", "sequences"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE INDEX",
				Objects: []wal.DDLObject{
					{Type: "index", Identity: "public.users_name_idx", Schema: "public"},
				},
			},
			wantSkip: true,
		},
		{
			name:    "fallback to command tag - no objects - function skipped",
			exclude: []string{"functions"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE FUNCTION",
				Objects:    nil,
			},
			wantSkip: true,
		},
		{
			name:    "fallback to command tag - no objects - table not skipped",
			exclude: []string{"functions"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "CREATE TABLE",
				Objects:    nil,
			},
			wantSkip: false,
		},
		{
			name:    "fallback to command tag - unknown tag - not skipped",
			exclude: []string{"functions"},
			ddlEvent: &wal.DDLEvent{
				CommandTag: "SOME_UNKNOWN_COMMAND",
				Objects:    nil,
			},
			wantSkip: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var f *ddlObjectTypeFilter
			if tc.include != nil || tc.exclude != nil {
				var err error
				f, err = newDDLObjectTypeFilter(tc.include, tc.exclude)
				require.NoError(t, err)
			}

			got := f.shouldSkipDDL(tc.ddlEvent)
			require.Equal(t, tc.wantSkip, got)
		})
	}
}
