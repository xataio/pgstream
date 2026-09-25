// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumn_IdentityValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		column  Column
		want    any
		comment string
	}{
		{
			name:   "a numeric keeps the identity a float64 produced",
			column: Column{Type: "numeric(10,2)", Value: "1.00"},
			want:   float64(1),
		},
		{
			name:   "a numeric with no modifier is the same",
			column: Column{Type: "numeric", Value: "2.50"},
			want:   2.5,
		},
		{
			name:   "a decimal is a numeric",
			column: Column{Type: "decimal(5,1)", Value: "3.0"},
			want:   float64(3),
		},
		{
			// NaN is not here: ParseFloat reads it, and the float64 renders
			// back as "NaN", which is the identity the previous version
			// produced. Nothing moves.
			name:   "a numeric that is not a number at all keeps its text",
			column: Column{Type: "numeric", Value: "not a number"},
			want:   "not a number",
		},
		{
			name:   "a text column is untouched",
			column: Column{Type: "text", Value: "1.00"},
			want:   "1.00",
		},
		{
			name:   "a bigint is untouched",
			column: Column{Type: "bigint", Value: int64(9007199254740993)},
			want:   int64(9007199254740993),
		},
		{
			name:   "a numeric that is not text is untouched",
			column: Column{Type: "numeric", Value: float64(4)},
			want:   float64(4),
		},
		{
			name:   "a nil value is untouched",
			column: Column{Type: "numeric", Value: nil},
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.column.IdentityValue())
		})
	}
}
