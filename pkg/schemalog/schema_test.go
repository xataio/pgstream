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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			col := tc.table.GetFirstUniqueNotNullColumn()
			require.Equal(t, tc.wantCol, col)
		})
	}
}

func TestColumn_GetSequenceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		column *Column
		want   string
	}{
		{
			name: "nil default value",
			column: &Column{
				DefaultValue: nil,
			},
			want: "",
		},
		{
			name: "empty default value",
			column: &Column{
				DefaultValue: stringPtr(""),
			},
			want: "",
		},
		{
			name: "valid sequence default value",
			column: &Column{
				DefaultValue: stringPtr("nextval('users_id_seq'::regclass)"),
			},
			want: "users_id_seq",
		},
		{
			name: "valid sequence with schema",
			column: &Column{
				DefaultValue: stringPtr("nextval('public.orders_id_seq'::regclass)"),
			},
			want: "public.orders_id_seq",
		},
		{
			name: "not a sequence default",
			column: &Column{
				DefaultValue: stringPtr("'default_value'"),
			},
			want: "",
		},
		{
			name: "invalid sequence format - missing prefix",
			column: &Column{
				DefaultValue: stringPtr("'users_id_seq'::regclass)"),
			},
			want: "",
		},
		{
			name: "invalid sequence format - missing suffix",
			column: &Column{
				DefaultValue: stringPtr("nextval('users_id_seq'"),
			},
			want: "",
		},
		{
			name: "partial match prefix",
			column: &Column{
				DefaultValue: stringPtr("nextval('users_id_seq'::text)"),
			},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.column.GetSequenceName()
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSequence_IsEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sequence *Sequence
		other    *Sequence
		want     bool
	}{
		{
			name:     "both nil",
			sequence: nil,
			other:    nil,
			want:     true,
		},
		{
			name:     "first nil, second not nil",
			sequence: nil,
			other:    &Sequence{Oid: "123", Name: "seq1"},
			want:     false,
		},
		{
			name:     "first not nil, second nil",
			sequence: &Sequence{Oid: "123", Name: "seq1"},
			other:    nil,
			want:     false,
		},
		{
			name: "identical sequences",
			sequence: &Sequence{
				Oid:                   "123",
				Name:                  "seq1",
				DataType:              stringPtr("bigint"),
				NumericPrecision:      int64Ptr(64),
				NumericPrecisionRadix: int64Ptr(2),
				NumericScale:          int64Ptr(0),
				StartValue:            stringPtr("1"),
				MinimumValue:          stringPtr("1"),
				MaximumValue:          stringPtr("9223372036854775807"),
				Increment:             stringPtr("1"),
				CycleOption:           stringPtr("NO"),
			},
			other: &Sequence{
				Oid:                   "123",
				Name:                  "seq1",
				DataType:              stringPtr("bigint"),
				NumericPrecision:      int64Ptr(64),
				NumericPrecisionRadix: int64Ptr(2),
				NumericScale:          int64Ptr(0),
				StartValue:            stringPtr("1"),
				MinimumValue:          stringPtr("1"),
				MaximumValue:          stringPtr("9223372036854775807"),
				Increment:             stringPtr("1"),
				CycleOption:           stringPtr("NO"),
			},
			want: true,
		},
		{
			name: "different oid",
			sequence: &Sequence{
				Oid:  "123",
				Name: "seq1",
			},
			other: &Sequence{
				Oid:  "456",
				Name: "seq1",
			},
			want: false,
		},
		{
			name: "different name",
			sequence: &Sequence{
				Oid:  "123",
				Name: "seq1",
			},
			other: &Sequence{
				Oid:  "123",
				Name: "seq2",
			},
			want: false,
		},
		{
			name: "different data type - one nil",
			sequence: &Sequence{
				Oid:      "123",
				Name:     "seq1",
				DataType: stringPtr("bigint"),
			},
			other: &Sequence{
				Oid:      "123",
				Name:     "seq1",
				DataType: nil,
			},
			want: false,
		},
		{
			name: "different data type values",
			sequence: &Sequence{
				Oid:      "123",
				Name:     "seq1",
				DataType: stringPtr("bigint"),
			},
			other: &Sequence{
				Oid:      "123",
				Name:     "seq1",
				DataType: stringPtr("integer"),
			},
			want: false,
		},
		{
			name: "different numeric precision",
			sequence: &Sequence{
				Oid:              "123",
				Name:             "seq1",
				NumericPrecision: int64Ptr(64),
			},
			other: &Sequence{
				Oid:              "123",
				Name:             "seq1",
				NumericPrecision: int64Ptr(32),
			},
			want: false,
		},
		{
			name: "different start value",
			sequence: &Sequence{
				Oid:        "123",
				Name:       "seq1",
				StartValue: stringPtr("1"),
			},
			other: &Sequence{
				Oid:        "123",
				Name:       "seq1",
				StartValue: stringPtr("100"),
			},
			want: false,
		},
		{
			name: "all nil pointer fields",
			sequence: &Sequence{
				Oid:  "123",
				Name: "seq1",
			},
			other: &Sequence{
				Oid:  "123",
				Name: "seq1",
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.sequence.IsEqual(tc.other)
			require.Equal(t, tc.want, got)
		})
	}
}

func int64Ptr(i int64) *int64 {
	return &i
}

func stringPtr(s string) *string {
	return &s
}
