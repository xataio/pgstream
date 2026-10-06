// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
)

// encodeArray returns the wire bytes postgres sends for an array value, which
// is what rows.RawValues() carries.
func encodeArray(t *testing.T, oid uint32, value any) []byte {
	t.Helper()
	raw, err := pgtype.NewMap().Encode(oid, pgtype.BinaryFormatCode, value, nil)
	require.NoError(t, err)
	return raw
}

func TestRowValues(t *testing.T) {
	t.Parallel()

	// {{1,2},{3,4}}: two dimensions of two elements.
	nested := pgtype.Array[int32]{
		Elements: []int32{1, 2, 3, 4},
		Dims: []pgtype.ArrayDimension{
			{Length: 2, LowerBound: 1},
			{Length: 2, LowerBound: 1},
		},
		Valid: true,
	}
	flat := pgtype.Array[int32]{
		Elements: []int32{1, 2, 3},
		Dims:     []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}},
		Valid:    true,
	}

	tests := []struct {
		name string
		oid  uint32
		raw  []byte
		// values is what rows.Values() gives. pgx flattens an array of more
		// than one dimension, which is the loss under test.
		values []any
		want   []any
	}{
		{
			// The shape has to survive. Without this the target holds
			// {1,2,3,4}, a one dimensional array, and nothing reports it.
			name:   "an array of two dimensions becomes its literal",
			oid:    pgtype.Int4ArrayOID,
			raw:    encodeArray(t, pgtype.Int4ArrayOID, nested),
			values: []any{[]any{int32(1), int32(2), int32(3), int32(4)}},
			want:   []any{"{{1,2},{3,4}}"},
		},
		{
			// One dimension already arrives correctly, so it keeps the type
			// rows.Values() gave it. A transformer and a non-postgres target
			// see no change.
			name:   "an array of one dimension is left alone",
			oid:    pgtype.Int4ArrayOID,
			raw:    encodeArray(t, pgtype.Int4ArrayOID, flat),
			values: []any{[]any{int32(1), int32(2), int32(3)}},
			want:   []any{[]any{int32(1), int32(2), int32(3)}},
		},
		{
			name:   "a column that is not an array is left alone",
			oid:    pgtype.TextOID,
			raw:    []byte("plain"),
			values: []any{"plain"},
			want:   []any{"plain"},
		},
		{
			// A NULL has no wire data to decode.
			name:   "a null array is left alone",
			oid:    pgtype.Int4ArrayOID,
			raw:    nil,
			values: []any{nil},
			want:   []any{nil},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rows := &pgmocks.Rows{
				ValuesFn:            func() ([]any, error) { return tc.values, nil },
				RawValuesFn:         func() [][]byte { return [][]byte{tc.raw} },
				FieldDescriptionsFn: func() []pgconn.FieldDescription { return fieldsFor(tc.oid) },
			}

			got, err := rowValues(rows)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestRowValues_rawValuesMissing covers the rows that carry no wire data. A
// reader that does not provide it must not lose the row.
func TestRowValues_rawValuesMissing(t *testing.T) {
	t.Parallel()

	values := []any{[]any{int32(1), int32(2)}}
	rows := &pgmocks.Rows{
		ValuesFn:            func() ([]any, error) { return values, nil },
		RawValuesFn:         func() [][]byte { return nil },
		FieldDescriptionsFn: func() []pgconn.FieldDescription { return fieldsFor(pgtype.Int4ArrayOID) },
	}

	got, err := rowValues(rows)
	require.NoError(t, err)
	require.Equal(t, values, got)
}

func fieldsFor(oid uint32) []pgconn.FieldDescription {
	return []pgconn.FieldDescription{{
		Name:         "a",
		DataTypeOID:  oid,
		Format:       pgtype.BinaryFormatCode,
		DataTypeSize: -1,
	}}
}

var _ pglib.Rows = (*pgmocks.Rows)(nil)
