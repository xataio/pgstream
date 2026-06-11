// SPDX-License-Identifier: Apache-2.0

package json

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnmarshalUseInt64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected any
	}{
		{
			name:     "small integer decodes as int64",
			input:    `{"value": 42}`,
			expected: int64(42),
		},
		{
			name:     "bigint above 2^53 preserved",
			input:    `{"value": 9007199254740993}`,
			expected: int64(9007199254740993),
		},
		{
			name:     "max int64 preserved",
			input:    `{"value": 9223372036854775807}`,
			expected: int64(9223372036854775807),
		},
		{
			name:     "negative bigint preserved",
			input:    `{"value": -9223372036854775808}`,
			expected: int64(-9223372036854775808),
		},
		{
			name:     "non-integer number decodes as float64",
			input:    `{"value": 1.5}`,
			expected: 1.5,
		},
		{
			// `1.0` is mathematically an integer but the JSON literal
			// contains a decimal point, so sonic treats it as float64.
			// Pin this down — Postgres serialises non-integer numerics
			// this way and pgstream relies on the type discriminator.
			name:     "integer-valued literal with decimal point stays float64",
			input:    `{"value": 1.0}`,
			expected: 1.0,
		},
		{
			// Scientific notation always decodes as float64 regardless of
			// whether the value is mathematically an integer.
			name:     "scientific notation decodes as float64",
			input:    `{"value": 1e10}`,
			expected: 1e10,
		},
		{
			name:     "negative scientific notation decodes as float64",
			input:    `{"value": -1.5e2}`,
			expected: -1.5e2,
		},
		{
			name:     "zero decodes as int64",
			input:    `{"value": 0}`,
			expected: int64(0),
		},
		{
			name:     "small negative integer decodes as int64",
			input:    `{"value": -1}`,
			expected: int64(-1),
		},
		{
			// 2^63 doesn't fit in int64; sonic falls back to float64
			// rather than wrapping around or returning an error.
			name:     "value above MaxInt64 falls back to float64",
			input:    `{"value": 9223372036854775808}`,
			expected: float64(9223372036854775808),
		},
		{
			name:     "null decodes as nil",
			input:    `{"value": null}`,
			expected: nil,
		},
		{
			// Sanity check that the rule applies inside arrays / nested
			// objects too — this is what makes the snapshot/jsonb fix
			// for #686 work end-to-end.
			name:     "nested large integer preserved through array",
			input:    `{"value": [1, 9223372036854775807, 3]}`,
			expected: []any{int64(1), int64(9223372036854775807), int64(3)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got map[string]any
			require.NoError(t, UnmarshalUseInt64([]byte(tt.input), &got))
			require.Equal(t, tt.expected, got["value"])
		})
	}
}
