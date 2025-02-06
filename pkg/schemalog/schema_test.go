// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
