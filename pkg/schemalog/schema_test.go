// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchema_Diff(t *testing.T) {
	t.Parallel()

	testTableName := "test-table"
	testTable := func() Table {
		return Table{
			PgstreamID: "1",
			Name:       testTableName,
			Columns: []Column{
				{PgstreamID: "1_1", Name: "col-1"},
			},
			PrimaryKeyColumns: []string{"col-1"},
		}
	}

	tests := []struct {
		name      string
		schema    Schema
		oldSchema Schema

		wantDiff *SchemaDiff
	}{
		{
			name: "columns to add",
			schema: Schema{
				Tables: []Table{testTable()},
			},
			oldSchema: Schema{
				Tables: []Table{
					{
						PgstreamID:        "1",
						Name:              testTableName,
						PrimaryKeyColumns: []string{"col-1"},
					},
				},
			},

			wantDiff: &SchemaDiff{
				ColumnsToAdd: []Column{
					{PgstreamID: "1_1", Name: "col-1"},
				},
			},
		},
		{
			name: "columns to add with non existing table",
			schema: Schema{
				Tables: []Table{testTable()},
			},
			oldSchema: Schema{
				Tables: []Table{},
			},

			wantDiff: &SchemaDiff{
				ColumnsToAdd: []Column{
					{PgstreamID: "1_1", Name: "col-1"},
				},
			},
		},
		{
			name: "tables to remove",
			schema: Schema{
				Tables: []Table{},
			},
			oldSchema: Schema{
				Tables: []Table{testTable()},
			},

			wantDiff: &SchemaDiff{
				TablesToRemove: []Table{testTable()},
			},
		},
		{
			name: "primary key changed",
			schema: Schema{
				Tables: []Table{testTable()},
			},
			oldSchema: Schema{
				Tables: []Table{
					{
						PgstreamID: "1",
						Name:       testTableName,
						Columns: []Column{
							{PgstreamID: "1_1", Name: "col-1"},
						},
					},
				},
			},

			wantDiff: &SchemaDiff{
				PrimaryKeyChange: []string{testTableName},
			},
		},
		{
			name: "unique not null changed",
			schema: Schema{
				Tables: []Table{
					{
						PgstreamID: "1",
						Name:       testTableName,
						Columns: []Column{
							{PgstreamID: "1_1", Name: "col-1", Unique: true, Nullable: false},
							{PgstreamID: "1_2", Name: "col-2", Unique: true, Nullable: false},
						},
					},
				},
			},
			oldSchema: Schema{
				Tables: []Table{
					{
						PgstreamID: "1",
						Name:       testTableName,
						Columns: []Column{
							{PgstreamID: "1_1", Name: "col-1", Unique: false, Nullable: true},
							{PgstreamID: "1_2", Name: "col-2", Unique: true, Nullable: false},
						},
					},
				},
			},

			wantDiff: &SchemaDiff{
				UniqueNotNullChange: []string{testTableName},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diff := tc.schema.Diff(&tc.oldSchema)
			require.Equal(t, tc.wantDiff, diff)
		})
	}
}

func TestTable_GetFirstUniqueNotNullColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table *Table

		wantCol *Column
	}{
		{
			name: "no unique not null columns",
			table: &Table{
				Columns: []Column{
					{PgstreamID: "1", Name: "col-1", Unique: false, Nullable: true},
					{PgstreamID: "2", Name: "col-2", Unique: true, Nullable: true},
					{PgstreamID: "3", Name: "col-3", Unique: false, Nullable: false},
				},
			},

			wantCol: nil,
		},
		{
			name: "single unique not null column",
			table: &Table{
				Columns: []Column{
					{PgstreamID: "1", Name: "col-1", Unique: false, Nullable: true},
					{PgstreamID: "2", Name: "col-2", Unique: true, Nullable: false},
					{PgstreamID: "3", Name: "col-3", Unique: false, Nullable: false},
				},
			},

			wantCol: &Column{PgstreamID: "2", Name: "col-2", Unique: true, Nullable: false},
		},
		{
			name: "multiple unique not null columns",
			table: &Table{
				Columns: []Column{
					{PgstreamID: "1", Name: "col-1", Unique: false, Nullable: true},
					{PgstreamID: "2", Name: "col-2", Unique: true, Nullable: false},
					{PgstreamID: "3", Name: "col-3", Unique: true, Nullable: false},
				},
			},

			wantCol: &Column{PgstreamID: "2", Name: "col-2", Unique: true, Nullable: false},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			col := tc.table.GetFirstUniqueNotNullColumn()
			require.Equal(t, tc.wantCol, col)
		})
	}
}
