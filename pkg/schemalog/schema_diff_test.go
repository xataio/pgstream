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

	testMaterializedView := func(name string, id string, indexes []Index) MaterializedView {
		return MaterializedView{
			Oid:        id,
			Name:       name,
			Definition: "SELECT id FROM test",
			Indexes:    indexes,
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
			name: "schema dropped",
			newSchema: &LogEntry{
				Schema: Schema{
					Dropped: true,
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{testTable(table1, id1)},
				},
			},

			wantDiff: &Diff{
				SchemaDropped: true,
			},
		},
		{
			name: "materialized views added and removed",
			newSchema: &LogEntry{
				Schema: Schema{
					MaterializedViews: []MaterializedView{testMaterializedView("mv_new", "2", nil)},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					MaterializedViews: []MaterializedView{testMaterializedView("mv_old", "1", nil)},
				},
			},

			wantDiff: &Diff{
				MaterializedViewsRemoved: []MaterializedView{testMaterializedView("mv_old", "1", nil)},
				MaterializedViewsAdded:   []MaterializedView{testMaterializedView("mv_new", "2", nil)},
			},
		},
		{
			name: "materialized view name changed",
			newSchema: &LogEntry{
				Schema: Schema{
					MaterializedViews: []MaterializedView{testMaterializedView("mv_new", "1", nil)},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					MaterializedViews: []MaterializedView{testMaterializedView("mv_old", "1", nil)},
				},
			},

			wantDiff: &Diff{
				MaterializedViewsChanged: []MaterializedViewsDiff{
					{
						MaterializedViewName: "mv_new",
						NameChange:           &ValueChange[string]{Old: "mv_old", New: "mv_new"},
					},
				},
			},
		},
		{
			name: "materialized view indexes added and removed",
			newSchema: &LogEntry{
				Schema: Schema{
					MaterializedViews: []MaterializedView{testMaterializedView("mv_test", "1", []Index{
						{Name: "idx_new", Definition: "CREATE INDEX idx_new ON mv_test USING btree (id)"},
					})},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					MaterializedViews: []MaterializedView{testMaterializedView("mv_test", "1", []Index{
						{Name: "idx_old", Definition: "CREATE INDEX idx_old ON mv_test USING btree (id)"},
					})},
				},
			},

			wantDiff: &Diff{
				MaterializedViewsChanged: []MaterializedViewsDiff{
					{
						MaterializedViewName: "mv_test",
						IndexesAdded: []Index{
							{Name: "idx_new", Definition: "CREATE INDEX idx_new ON mv_test USING btree (id)"},
						},
						IndexesRemoved: []Index{
							{Name: "idx_old", Definition: "CREATE INDEX idx_old ON mv_test USING btree (id)"},
						},
					},
				},
			},
		},
		{
			name: "materialized view indexes changed",
			newSchema: &LogEntry{
				Schema: Schema{
					MaterializedViews: []MaterializedView{testMaterializedView("mv_test", "1", []Index{
						{Name: "idx_old", Definition: "ALTER INDEX idx_old RENAME TO idx_new"},
					})},
				},
			},
			oldSchema: &LogEntry{
				Schema: Schema{
					MaterializedViews: []MaterializedView{testMaterializedView("mv_test", "1", []Index{
						{Name: "idx_old", Definition: "CREATE INDEX idx_old ON mv_test USING btree (id)"},
					})},
				},
			},

			wantDiff: &Diff{
				MaterializedViewsChanged: []MaterializedViewsDiff{
					{
						MaterializedViewName: "mv_test",
						IndexesChanged:       []string{"ALTER INDEX idx_old RENAME TO idx_new"},
					},
				},
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
			name: "constraints added and removed",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: id1,
							Name:       table1,
							Columns: []Column{
								{PgstreamID: id1 + "_1", Name: col1},
							},
							PrimaryKeyColumns: []string{"col-1"},
							Constraints: []Constraint{
								{Name: "unique_name", Type: "UNIQUE", Definition: "UNIQUE (name)"},
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
							PrimaryKeyColumns: []string{"col-1"},
							Constraints: []Constraint{
								{Name: "old_check", Type: "CHECK", Definition: "CHECK (id > 0)"},
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
						ConstraintsAdded: []Constraint{
							{Name: "unique_name", Type: "UNIQUE", Definition: "UNIQUE (name)"},
						},
						ConstraintsRemoved: []Constraint{
							{Name: "old_check", Type: "CHECK", Definition: "CHECK (id > 0)"},
						},
					},
				},
			},
		},
		{
			name: "foreign keys added and removed",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: id1,
							Name:       table1,
							Columns: []Column{
								{PgstreamID: id1 + "_1", Name: col1},
							},
							PrimaryKeyColumns: []string{"col-1"},
							ForeignKeys: []ForeignKey{
								{Name: "fk_new", Definition: "FOREIGN KEY (col_1) REFERENCES other(id)"},
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
							PrimaryKeyColumns: []string{"col-1"},
							ForeignKeys: []ForeignKey{
								{Name: "fk_old", Definition: "FOREIGN KEY (col_1) REFERENCES another(id)"},
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
						ForeignKeysAdded: []ForeignKey{
							{Name: "fk_new", Definition: "FOREIGN KEY (col_1) REFERENCES other(id)"},
						},
						ForeignKeysRemoved: []ForeignKey{
							{Name: "fk_old", Definition: "FOREIGN KEY (col_1) REFERENCES another(id)"},
						},
					},
				},
			},
		},
		{
			name: "index altered via definition",
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
									Name:       "idx_old",
									Columns:    []string{col1},
									Definition: "ALTER INDEX idx_old RENAME TO idx_new",
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
						IndexesChanged: []string{
							"ALTER INDEX idx_old RENAME TO idx_new",
						},
					},
				},
			},
		},
		{
			name: "index definition changed",
			newSchema: &LogEntry{
				Schema: Schema{
					Tables: []Table{
						{
							PgstreamID: id1,
							Name:       table1,
							Columns: []Column{
								{PgstreamID: id1 + "_1", Name: col1},
								{PgstreamID: id1 + "_2", Name: col2},
							},
							PrimaryKeyColumns: []string{col1},
							Indexes: []Index{
								{
									Name:       "idx_new",
									Columns:    []string{col1, col2},
									Definition: "CREATE INDEX idx_new ON test.table USING btree (col-1, col-2)",
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
								{PgstreamID: id1 + "_2", Name: col2},
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
								Columns:    []string{col1, col2},
								Definition: "CREATE INDEX idx_new ON test.table USING btree (col-1, col-2)",
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

func Test_Diff_IsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		diff     *Diff
		expected bool
	}{
		{
			name:     "empty diff",
			diff:     &Diff{},
			expected: true,
		},
		{
			name: "schema dropped",
			diff: &Diff{
				SchemaDropped: true,
			},
			expected: false,
		},
		{
			name: "tables added",
			diff: &Diff{
				TablesAdded: []Table{
					{Name: "test_table", PgstreamID: "1"},
				},
			},
			expected: false,
		},
		{
			name: "tables removed",
			diff: &Diff{
				TablesRemoved: []Table{
					{Name: "test_table", PgstreamID: "1"},
				},
			},
			expected: false,
		},
		{
			name: "tables changed",
			diff: &Diff{
				TablesChanged: []TableDiff{
					{TableName: "test_table", TablePgstreamID: "1"},
				},
			},
			expected: false,
		},
		{
			name: "materialized views added",
			diff: &Diff{
				MaterializedViewsAdded: []MaterializedView{
					{Name: "test_mv", Oid: "1"},
				},
			},
			expected: false,
		},
		{
			name: "materialized views removed",
			diff: &Diff{
				MaterializedViewsRemoved: []MaterializedView{
					{Name: "test_mv", Oid: "1"},
				},
			},
			expected: false,
		},
		{
			name: "materialized views changed",
			diff: &Diff{
				MaterializedViewsChanged: []MaterializedViewsDiff{
					{MaterializedViewName: "test_mv"},
				},
			},
			expected: false,
		},
		{
			name: "multiple changes",
			diff: &Diff{
				SchemaDropped: true,
				TablesAdded: []Table{
					{Name: "test_table", PgstreamID: "1"},
				},
				MaterializedViewsRemoved: []MaterializedView{
					{Name: "test_mv", Oid: "1"},
				},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := tc.diff.IsEmpty()
			require.Equal(t, tc.expected, result)
		})
	}
}

func Test_computeSequenceDiff(t *testing.T) {
	t.Parallel()

	increment1 := "1"
	increment2 := "5"
	minValue1 := "1"
	minValue2 := "10"
	maxValue1 := "1000"
	maxValue2 := "9999"
	startValue1 := "1"
	startValue2 := "100"
	cycleOption1 := "NO"
	cycleOption2 := "YES"
	dataType1 := "bigint"
	dataType2 := "integer"

	tests := []struct {
		name        string
		oldSequence *Sequence
		newSequence *Sequence
		wantDiff    *SequenceDiff
	}{
		{
			name: "no changes",
			oldSequence: &Sequence{
				Name: "seq1",
				Oid:  "1001",
			},
			newSequence: &Sequence{
				Name: "seq1",
				Oid:  "1001",
			},
			wantDiff: &SequenceDiff{
				SequenceName: "seq1",
			},
		},
		{
			name: "name change",
			oldSequence: &Sequence{
				Name: "seq_old",
				Oid:  "1001",
			},
			newSequence: &Sequence{
				Name: "seq_new",
				Oid:  "1001",
			},
			wantDiff: &SequenceDiff{
				SequenceName: "seq_new",
				NameChange:   &ValueChange[string]{Old: "seq_old", New: "seq_new"},
			},
		},
		{
			name: "data type change",
			oldSequence: &Sequence{
				Name:     "seq1",
				Oid:      "1001",
				DataType: &dataType1,
			},
			newSequence: &Sequence{
				Name:     "seq1",
				Oid:      "1001",
				DataType: &dataType2,
			},
			wantDiff: &SequenceDiff{
				SequenceName:   "seq1",
				DataTypeChange: &ValueChange[*string]{Old: &dataType1, New: &dataType2},
			},
		},
		{
			name: "increment change",
			oldSequence: &Sequence{
				Name:      "seq1",
				Oid:       "1001",
				Increment: &increment1,
			},
			newSequence: &Sequence{
				Name:      "seq1",
				Oid:       "1001",
				Increment: &increment2,
			},
			wantDiff: &SequenceDiff{
				SequenceName:    "seq1",
				IncrementChange: &ValueChange[*string]{Old: &increment1, New: &increment2},
			},
		},
		{
			name: "minimum value change",
			oldSequence: &Sequence{
				Name:         "seq1",
				Oid:          "1001",
				MinimumValue: &minValue1,
			},
			newSequence: &Sequence{
				Name:         "seq1",
				Oid:          "1001",
				MinimumValue: &minValue2,
			},
			wantDiff: &SequenceDiff{
				SequenceName:       "seq1",
				MinimumValueChange: &ValueChange[*string]{Old: &minValue1, New: &minValue2},
			},
		},
		{
			name: "maximum value change",
			oldSequence: &Sequence{
				Name:         "seq1",
				Oid:          "1001",
				MaximumValue: &maxValue1,
			},
			newSequence: &Sequence{
				Name:         "seq1",
				Oid:          "1001",
				MaximumValue: &maxValue2,
			},
			wantDiff: &SequenceDiff{
				SequenceName:       "seq1",
				MaximumValueChange: &ValueChange[*string]{Old: &maxValue1, New: &maxValue2},
			},
		},
		{
			name: "start value change",
			oldSequence: &Sequence{
				Name:       "seq1",
				Oid:        "1001",
				StartValue: &startValue1,
			},
			newSequence: &Sequence{
				Name:       "seq1",
				Oid:        "1001",
				StartValue: &startValue2,
			},
			wantDiff: &SequenceDiff{
				SequenceName:     "seq1",
				StartValueChange: &ValueChange[*string]{Old: &startValue1, New: &startValue2},
			},
		},
		{
			name: "cycle option change",
			oldSequence: &Sequence{
				Name:        "seq1",
				Oid:         "1001",
				CycleOption: &cycleOption1,
			},
			newSequence: &Sequence{
				Name:        "seq1",
				Oid:         "1001",
				CycleOption: &cycleOption2,
			},
			wantDiff: &SequenceDiff{
				SequenceName:      "seq1",
				CycleOptionChange: &ValueChange[*string]{Old: &cycleOption1, New: &cycleOption2},
			},
		},
		{
			name: "multiple changes",
			oldSequence: &Sequence{
				Name:         "seq_old",
				Oid:          "1001",
				DataType:     &dataType1,
				Increment:    &increment1,
				MinimumValue: &minValue1,
				MaximumValue: &maxValue1,
				StartValue:   &startValue1,
				CycleOption:  &cycleOption1,
			},
			newSequence: &Sequence{
				Name:         "seq_new",
				Oid:          "1001",
				DataType:     &dataType2,
				Increment:    &increment2,
				MinimumValue: &minValue2,
				MaximumValue: &maxValue2,
				StartValue:   &startValue2,
				CycleOption:  &cycleOption2,
			},
			wantDiff: &SequenceDiff{
				SequenceName:       "seq_new",
				NameChange:         &ValueChange[string]{Old: "seq_old", New: "seq_new"},
				DataTypeChange:     &ValueChange[*string]{Old: &dataType1, New: &dataType2},
				IncrementChange:    &ValueChange[*string]{Old: &increment1, New: &increment2},
				MinimumValueChange: &ValueChange[*string]{Old: &minValue1, New: &minValue2},
				MaximumValueChange: &ValueChange[*string]{Old: &maxValue1, New: &maxValue2},
				StartValueChange:   &ValueChange[*string]{Old: &startValue1, New: &startValue2},
				CycleOptionChange:  &ValueChange[*string]{Old: &cycleOption1, New: &cycleOption2},
			},
		},
		{
			name: "nil to value changes",
			oldSequence: &Sequence{
				Name: "seq1",
				Oid:  "1001",
			},
			newSequence: &Sequence{
				Name:         "seq1",
				Oid:          "1001",
				DataType:     &dataType1,
				Increment:    &increment1,
				MinimumValue: &minValue1,
				MaximumValue: &maxValue1,
				StartValue:   &startValue1,
				CycleOption:  &cycleOption1,
			},
			wantDiff: &SequenceDiff{
				SequenceName:       "seq1",
				DataTypeChange:     &ValueChange[*string]{Old: nil, New: &dataType1},
				IncrementChange:    &ValueChange[*string]{Old: nil, New: &increment1},
				MinimumValueChange: &ValueChange[*string]{Old: nil, New: &minValue1},
				MaximumValueChange: &ValueChange[*string]{Old: nil, New: &maxValue1},
				StartValueChange:   &ValueChange[*string]{Old: nil, New: &startValue1},
				CycleOptionChange:  &ValueChange[*string]{Old: nil, New: &cycleOption1},
			},
		},
		{
			name: "value to nil changes",
			oldSequence: &Sequence{
				Name:         "seq1",
				Oid:          "1001",
				DataType:     &dataType1,
				Increment:    &increment1,
				MinimumValue: &minValue1,
				MaximumValue: &maxValue1,
				StartValue:   &startValue1,
				CycleOption:  &cycleOption1,
			},
			newSequence: &Sequence{
				Name: "seq1",
				Oid:  "1001",
			},
			wantDiff: &SequenceDiff{
				SequenceName:       "seq1",
				DataTypeChange:     &ValueChange[*string]{Old: &dataType1, New: nil},
				IncrementChange:    &ValueChange[*string]{Old: &increment1, New: nil},
				MinimumValueChange: &ValueChange[*string]{Old: &minValue1, New: nil},
				MaximumValueChange: &ValueChange[*string]{Old: &maxValue1, New: nil},
				StartValueChange:   &ValueChange[*string]{Old: &startValue1, New: nil},
				CycleOptionChange:  &ValueChange[*string]{Old: &cycleOption1, New: nil},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diff := computeSequenceDiff(tc.oldSequence, tc.newSequence)
			require.Equal(t, tc.wantDiff, diff)
		})
	}
}

func Test_SequenceDiff_IsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		diff      *SequenceDiff
		wantEmpty bool
	}{
		{
			name: "empty diff",
			diff: &SequenceDiff{
				SequenceName: "seq1",
			},
			wantEmpty: true,
		},
		{
			name: "has name change",
			diff: &SequenceDiff{
				SequenceName: "seq1",
				NameChange:   &ValueChange[string]{Old: "old", New: "new"},
			},
			wantEmpty: false,
		},
		{
			name: "has data type change",
			diff: &SequenceDiff{
				SequenceName:   "seq1",
				DataTypeChange: &ValueChange[*string]{Old: nil, New: stringPtr("bigint")},
			},
			wantEmpty: false,
		},
		{
			name: "has increment change",
			diff: &SequenceDiff{
				SequenceName:    "seq1",
				IncrementChange: &ValueChange[*string]{Old: stringPtr("1"), New: stringPtr("5")},
			},
			wantEmpty: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			isEmpty := tc.diff.IsEmpty()
			require.Equal(t, tc.wantEmpty, isEmpty)
		})
	}
}
