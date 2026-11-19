// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/pkg/wal"
)

func TestNumberValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		number  string
		colType string
		want    any
	}{
		{
			name:    "a numeric keeps every digit",
			number:  "106.000000000000000001",
			colType: "numeric",
			want:    "106.000000000000000001",
		},
		{
			name:    "a numeric wider than a float64 keeps every digit",
			number:  "123456789012345678901234567890.123456789",
			colType: "numeric",
			want:    "123456789012345678901234567890.123456789",
		},
		{
			name:    "a numeric next to the bigint bound keeps its value",
			number:  "-9223372036854775808.5",
			colType: "numeric",
			want:    "-9223372036854775808.5",
		},
		{
			name:    "a bigint becomes an int64",
			number:  "9223372036854775807",
			colType: "bigint",
			want:    int64(9223372036854775807),
		},
		{
			name:    "an integer becomes an int64",
			number:  "-42",
			colType: "integer",
			want:    int64(-42),
		},
		{
			name:    "a double precision becomes a float64",
			number:  "0.1",
			colType: "double precision",
			want:    0.1,
		},
		{
			name:    "an unknown type keeps its text",
			number:  "1.5",
			colType: "money",
			want:    "1.5",
		},
		{
			name:    "an integer column with a value that is not one keeps its text",
			number:  "1.5",
			colType: "integer",
			want:    "1.5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, numberValue(json.Number(tc.number), tc.colType))
		})
	}
}

func TestDecodeNumberColumns(t *testing.T) {
	t.Parallel()

	data := &wal.Data{
		Columns: []wal.Column{
			{Name: "n", Type: "numeric", Value: json.Number("0.10000000000000000000000000001")},
			{Name: "i", Type: "bigint", Value: json.Number("9007199254740993")},
			{Name: "s", Type: "text", Value: "unchanged"},
		},
		Identity: []wal.Column{
			{Name: "n", Type: "numeric", Value: json.Number("1.0000000000000000000001")},
		},
	}

	decodeNumberColumns(data)

	require.Equal(t, "0.10000000000000000000000000001", data.Columns[0].Value)
	require.Equal(t, int64(9007199254740993), data.Columns[1].Value)
	require.Equal(t, "unchanged", data.Columns[2].Value)
	require.Equal(t, "1.0000000000000000000001", data.Identity[0].Value)
}

func TestDecodeNumberColumns_NilData(t *testing.T) {
	t.Parallel()
	require.NotPanics(t, func() { decodeNumberColumns(nil) })
}
