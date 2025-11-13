// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	table1 = "table-1"
	table2 = "table-2"
	col1   = "col-1"
	col2   = "col-2"
	id1    = "1"
	id2    = "2"
)

func Test_ComputeSchemaDiff(t *testing.T) {
	t.Parallel()

	testTable := func(name, id string, cols ...[]Column) Table {
		tableCols := []Column{
			{PgstreamID: id + "_1", Name: "col-1"},
		}
		if len(cols) > 0 {
			tableCols = cols[0]
		}
		return Table{
			PgstreamID:        id,
			Name:              name,
			Columns:           tableCols,
			PrimaryKeyColumns: []string{"col-1"},
		}
	}

	tests := []struct {
		name      string
		newSchema *LogEntry
		oldSchema *LogEntry

		wantDiff *Diff
	}{
		{
			name: "no diff",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1)},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1)},
				},
			},

			wantDiff: &Diff{},
		},
		{
			name:      "no diff - no schemas",
			newSchema: nil,
			oldSchema: nil,

			wantDiff: &Diff{},
		},
		{
			name: "no old schema",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1)},
				},
			},
			oldSchema: nil,

			wantDiff: &Diff{
				TablesAdded: []Table{testTable(table1, id1)},
			},
		},
		{
			name:      "no new schema",
			newSchema: nil,
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1)},
				},
			},

			wantDiff: &Diff{
				TablesRemoved: []Table{testTable(table1, id1)},
			},
		},
		{
			name: "table added and removed",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table2, id2)},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1)},
				},
			},

			wantDiff: &Diff{
				TablesRemoved: []Table{testTable(table1, id1)},
				TablesAdded:   []Table{testTable(table2, id2)},
			},
		},
		{
			name: "table name changed",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table2, id1)},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1)},
				},
			},

			wantDiff: &Diff{
				TablesChanged: []TableDiff{
					{
						TableName:       table2,
						TablePgstreamID: id1,
						TableNameChange: &ValueChange[string]{Old: table1, New: table2},
					},
				},
			},
		},
		{
			name: "table primary key changed",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: id1,
							Name:       table1,
							Columns: []Column{
								{PgstreamID: id1 + "_1", Name: "col-1"},
								{PgstreamID: id1 + "_2", Name: "col-2"},
							},
							PrimaryKeyColumns: []string{"col-2"},
						},
					},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: id1,
							Name:       table1,
							Columns: []Column{
								{PgstreamID: id1 + "_1", Name: "col-1"},
								{PgstreamID: id1 + "_2", Name: "col-2"},
							},
							PrimaryKeyColumns: []string{"col-1"},
						},
					},
				},
			},

			wantDiff: &Diff{
				TablesChanged: []TableDiff{
					{
						TableName:             table1,
						TablePgstreamID:       id1,
						TablePrimaryKeyChange: &ValueChange[[]string]{Old: []string{"col-1"}, New: []string{"col-2"}},
					},
				},
			},
		},
		{
			name: "columns added and removed",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1, []Column{
						{
							Name:       col2,
							PgstreamID: id2,
						},
					})},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1, []Column{
						{
							Name:       col1,
							PgstreamID: id1,
						},
					})},
				},
			},

			wantDiff: &Diff{
				TablesChanged: []TableDiff{
					{
						TableName:       table1,
						TablePgstreamID: id1,
						ColumnsAdded: []Column{
							{
								Name:       col2,
								PgstreamID: id2,
							},
						},
						ColumnsRemoved: []Column{
							{
								Name:       col1,
								PgstreamID: id1,
							},
						},
					},
				},
			},
		},
		{
			name: "indexes added and removed",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: id1,
							Name:       table1,
							Columns: []Column{
								{PgstreamID: id1 + "_1", Name: col1},
							},
							PrimaryKeyColumns: []string{col1},
							Indexes: []Index{
								{
									Name:       "idx_new",
									Columns:    []string{col1},
									Definition: "CREATE INDEX idx_new ON test.table USING btree (col-1)",
								},
							},
						},
					},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: id1,
							Name:       table1,
							Columns: []Column{
								{PgstreamID: id1 + "_1", Name: col1},
							},
							PrimaryKeyColumns: []string{col1},
							Indexes: []Index{
								{
									Name:       "idx_old",
									Columns:    []string{col1},
									Definition: "CREATE INDEX idx_old ON test.table USING btree (col-1)",
								},
							},
						},
					},
				},
			},

			wantDiff: &Diff{
				TablesChanged: []TableDiff{
					{
						TableName:       table1,
						TablePgstreamID: id1,
						IndexesAdded: []Index{
							{
								Name:       "idx_new",
								Columns:    []string{col1},
								Definition: "CREATE INDEX idx_new ON test.table USING btree (col-1)",
							},
						},
						IndexesRemoved: []Index{
							{
								Name:       "idx_old",
								Columns:    []string{col1},
								Definition: "CREATE INDEX idx_old ON test.table USING btree (col-1)",
							},
						},
					},
				},
			},
		},
		{
			name: "column name change",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1, []Column{
						{
							Name:       col2,
							PgstreamID: id1,
						},
					})},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1, []Column{
						{
							Name:       col1,
							PgstreamID: id1,
						},
					})},
				},
			},

			wantDiff: &Diff{
				TablesChanged: []TableDiff{
					{
						TableName:       table1,
						TablePgstreamID: id1,
						ColumnsChanged: []ColumnDiff{
							{
								ColumnName:       col2,
								ColumnPgstreamID: id1,
								NameChange:       &ValueChange[string]{Old: col1, New: col2},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diff := ComputeSchemaDiff(tc.oldSchema, tc.newSchema)
			require.Equal(t, tc.wantDiff, diff)
		})
	}
}

func Test_computeColumnDiff(t *testing.T) {
	t.Parallel()

	default1 := "1"

	tests := []struct {
		name      string
		oldColumn *Column
		newColumn *Column

		wantDiff *ColumnDiff
	}{
		{
			name: "name change",
			oldColumn: &Column{
				Name:       col1,
				PgstreamID: id1,
			},
			newColumn: &Column{
				Name:       col2,
				PgstreamID: id1,
			},

			wantDiff: &ColumnDiff{
				ColumnName:       col2,
				ColumnPgstreamID: id1,
				NameChange:       &ValueChange[string]{Old: col1, New: col2},
			},
		},
		{
			name: "type change",
			oldColumn: &Column{
				Name:       col1,
				PgstreamID: id1,
				DataType:   "int",
			},
			newColumn: &Column{
				Name:       col1,
				PgstreamID: id1,
				DataType:   "text",
			},

			wantDiff: &ColumnDiff{
				ColumnName:       col1,
				ColumnPgstreamID: id1,
				TypeChange:       &ValueChange[string]{Old: "int", New: "text"},
			},
		},
		{
			name: "default change",
			oldColumn: &Column{
				Name:         col1,
				PgstreamID:   id1,
				DefaultValue: nil,
			},
			newColumn: &Column{
				Name:         col1,
				PgstreamID:   id1,
				DefaultValue: &default1,
			},

			wantDiff: &ColumnDiff{
				ColumnName:       col1,
				ColumnPgstreamID: id1,
				DefaultChange:    &ValueChange[*string]{Old: nil, New: &default1},
			},
		},
		{
			name: "unique change",
			oldColumn: &Column{
				Name:       col1,
				PgstreamID: id1,
				Unique:     true,
			},
			newColumn: &Column{
				Name:       col1,
				PgstreamID: id1,
				Unique:     false,
			},

			wantDiff: &ColumnDiff{
				ColumnName:       col1,
				ColumnPgstreamID: id1,
				UniqueChange:     &ValueChange[bool]{Old: true, New: false},
			},
		},
		{
			name: "null change",
			oldColumn: &Column{
				Name:       col1,
				PgstreamID: id1,
				Nullable:   true,
			},
			newColumn: &Column{
				Name:       col1,
				PgstreamID: id1,
				Nullable:   false,
			},

			wantDiff: &ColumnDiff{
				ColumnName:       col1,
				ColumnPgstreamID: id1,
				NullChange:       &ValueChange[bool]{Old: true, New: false},
			},
		},
		{
			name: "multiple changes",
			oldColumn: &Column{
				Name:         col1,
				PgstreamID:   id1,
				DataType:     "int",
				DefaultValue: nil,
				Unique:       true,
				Nullable:     true,
			},
			newColumn: &Column{
				Name:         col2,
				PgstreamID:   id1,
				DataType:     "text",
				DefaultValue: &default1,
				Unique:       false,
				Nullable:     false,
			},

			wantDiff: &ColumnDiff{
				ColumnName:       col2,
				ColumnPgstreamID: id1,
				NameChange:       &ValueChange[string]{Old: col1, New: col2},
				TypeChange:       &ValueChange[string]{Old: "int", New: "text"},
				DefaultChange:    &ValueChange[*string]{Old: nil, New: &default1},
				UniqueChange:     &ValueChange[bool]{Old: true, New: false},
				NullChange:       &ValueChange[bool]{Old: true, New: false},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diff := computeColumnDiff(tc.oldColumn, tc.newColumn)
			require.Equal(t, tc.wantDiff, diff)
		})
	}
}
