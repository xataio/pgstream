// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogEntry_Diff(t *testing.T) {
	nopDiff := &SchemaDiff{}

	tcs := map[string]struct {
		old  *LogEntry
		new  *LogEntry
		want *SchemaDiff
	}{
		"table removed": {
			old: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{PgstreamID: "1"},
					},
				},
			},
			new: &LogEntry{
				Schema: Schema{
					Tables: []Table{},
				},
			},
			want: &SchemaDiff{
				TablesToRemove: []Table{
					{PgstreamID: "1"},
				},
			},
		},
		"table added without columns": {
			old: &LogEntry{
				Schema: Schema{
					Tables: []Table{},
				},
			},
			new: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{PgstreamID: "1"},
					},
				},
			},
			want: nopDiff,
		},
		"table renamed": {
			old: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{PgstreamID: "1", Name: "old"},
					},
				},
			},
			new: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{PgstreamID: "1", Name: "new"},
					},
				},
			},
			want: nopDiff,
		},
		"new table added with columns": {
			old: &LogEntry{
				Schema: Schema{
					Tables: []Table{},
				},
			},
			new: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: "1",
							Columns: []Column{
								{Name: "col1", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
								{Name: "col2", PgstreamID: "1-2", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
							},
						},
					},
				},
			},
			want: &SchemaDiff{
				ColumnsToAdd: []Column{
					{Name: "col1", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
					{Name: "col2", PgstreamID: "1-2", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
				},
			},
		},
		"column added": {
			old: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: "1",
							Columns: []Column{
								{Name: "col1", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
							},
						},
					},
				},
			},
			new: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: "1",
							Columns: []Column{
								{Name: "col1", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
								{Name: "col2", PgstreamID: "1-2", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
							},
						},
					},
				},
			},
			want: &SchemaDiff{
				ColumnsToAdd: []Column{
					{Name: "col2", PgstreamID: "1-2", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
				},
			},
		},
		"column removed": {
			old: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: "1",
							Columns: []Column{
								{Name: "col1", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
							},
						},
					},
				},
			},
			new: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: "1",
							Columns:    []Column{},
						},
					},
				},
			},
			want: nopDiff,
		},
		"table and columns removed": {
			old: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: "1",
							Columns: []Column{
								{Name: "col1", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
							},
						},
					},
				},
			},
			new: &LogEntry{
				Schema: Schema{
					Tables: []Table{},
				},
			},
			want: &SchemaDiff{
				TablesToRemove: []Table{
					{
						PgstreamID: "1",
						Columns: []Column{
							{Name: "col1", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
						},
					},
				},
			},
		},
		"column renamed": {
			old: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: "1",
							Columns: []Column{
								{Name: "col1", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
							},
						},
					},
				},
			},
			new: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: "1",
							Columns: []Column{
								{Name: "col1-renamed", PgstreamID: "1-1", DataType: "text", Nullable: true, DefaultValue: ptr("a")},
							},
						},
					},
				},
			},
			want: nopDiff,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			got := tc.new.Diff(tc.old)
			require.Equal(t, tc.want, got)
		})
	}
}

func ptr[T any](in T) *T {
	return &in
}
