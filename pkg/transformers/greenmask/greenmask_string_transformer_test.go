// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewStringTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params transformers.Parameters

		wantErr error
	}{
		{
			name: "ok - random",
			params: map[string]any{
				"generator":  random,
				"symbols":    "abcdef",
				"min_length": int(2),
				"max_length": int(2),
			},

			wantErr: nil,
		},
		{
			name: "ok - deterministic",
			params: map[string]any{
				"generator":  deterministic,
				"symbols":    "abcdef",
				"min_length": int(2),
				"max_length": int(2),
			},

			wantErr: nil,
		},
		{
			name: "error - invalid symbols",
			params: map[string]any{
				"generator": random,
				"symbols":   123,
			},

			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid min length",
			params: map[string]any{
				"generator":  random,
				"min_length": "2",
			},

			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max length",
			params: map[string]any{
				"generator":  random,
				"max_length": "2",
			},

			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid generator",
			params: map[string]any{
				"generator": "invalid",
			},

			wantErr: transformers.ErrUnsupportedGenerator,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewStringTransformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStringTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		value  any
		params transformers.Parameters

		wantLen int
		wantStr string
		wantErr error
	}{
		{
			name:  "ok - random with string",
			value: "hello",
			params: map[string]any{
				"generator":  random,
				"symbols":    "abcdef",
				"min_length": int(2),
				"max_length": int(2),
			},

			wantLen: 2,
			wantErr: nil,
		},
		{
			name:  "ok - random with []byte",
			value: []byte("hello"),
			params: map[string]any{
				"generator":  random,
				"symbols":    "abcdef",
				"min_length": int(2),
				"max_length": int(2),
			},

			wantLen: 2,
			wantErr: nil,
		},
		{
			name:  "ok - deterministic",
			value: "hello",
			params: map[string]any{
				"generator":  deterministic,
				"symbols":    "abcdef",
				"min_length": int(2),
				"max_length": int(2),
			},

			wantLen: 2,
			wantStr: "dc",
			wantErr: nil,
		},
		{
			name:  "error - unsupported value type",
			value: 1,
			params: map[string]any{
				"generator":  random,
				"symbols":    "abcdef",
				"min_length": int(2),
				"max_length": int(2),
			},

			wantLen: 0,
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewStringTransformer(tc.params)
			require.NoError(t, err)

			got, err := transformer.Transform(tc.value)
			require.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}

			require.Len(t, got, tc.wantLen)
			if tc.wantStr == "" {
				return
			}
			require.Equal(t, tc.wantStr, got)
		})
	}
}
