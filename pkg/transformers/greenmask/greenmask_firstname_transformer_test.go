// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewFirstNameTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params transformers.Parameters

		wantErr error
	}{
		{
			name: "ok - random",
			params: map[string]any{
				"generator": random,
				"gender":    "female",
			},

			wantErr: nil,
		},
		{
			name: "ok - deterministic",
			params: map[string]any{
				"generator": deterministic,
				"gender":    "male",
			},

			wantErr: nil,
		},
		{
			name: "ok - unknown gender defaults to any",
			params: map[string]any{
				"generator": deterministic,
				"gender":    "other",
			},

			wantErr: nil,
		},
		{
			name: "error - invalid gender",
			params: map[string]any{
				"generator": random,
				"gender":    1,
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

			_, err := NewFirstNameTransformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestFirstNameTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		value  any
		params transformers.Parameters

		wantName string
		wantErr  error
	}{
		{
			name:  "ok - random with string",
			value: "alice",
			params: map[string]any{
				"generator": random,
				"gender":    "female",
			},

			wantErr: nil,
		},
		{
			name:  "ok - random with []byte",
			value: []byte("alice"),
			params: map[string]any{
				"generator": random,
				"gender":    "male",
			},

			wantErr: nil,
		},
		{
			name:  "ok - deterministic",
			value: "alice",
			params: map[string]any{
				"generator": deterministic,
				"gender":    "female",
			},

			wantName: "Pearlie",
			wantErr:  nil,
		},
		{
			name:  "error - unsupported value type",
			value: 1,
			params: map[string]any{
				"generator": random,
				"gender":    "female",
			},

			wantErr: transformers.ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewFirstNameTransformer(tc.params)
			require.NoError(t, err)

			require.ErrorIs(t, err, tc.wantErr)
			got, err := transformer.Transform(transformers.NewValue(tc.value, nil))
			if err != nil {
				return
			}

			if tc.wantName == "" {
				return
			}
			require.Equal(t, tc.wantName, got)
		})
	}
}
