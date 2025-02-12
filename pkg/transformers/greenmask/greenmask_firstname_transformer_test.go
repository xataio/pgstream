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
		name          string
		params        transformers.Parameters
		generatorType transformers.GeneratorType

		wantErr error
	}{
		{
			name: "ok - random",
			params: map[string]any{
				"gender": "female",
			},
			generatorType: transformers.Random,

			wantErr: nil,
		},
		{
			name: "ok - deterministic",
			params: map[string]any{
				"gender": "male",
			},
			generatorType: transformers.Deterministic,

			wantErr: nil,
		},
		{
			name: "ok - unknown gender defaults to any",
			params: map[string]any{
				"gender": "other",
			},
			generatorType: transformers.Deterministic,

			wantErr: nil,
		},
		{
			name: "error - invalid gender",
			params: map[string]any{
				"gender": 1,
			},
			generatorType: transformers.Random,

			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name:          "error - invalid generator",
			params:        map[string]any{},
			generatorType: "invalid",

			wantErr: transformers.ErrUnsupportedGenerator,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewFirstNameTransformer(tc.generatorType, tc.params)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestFirstNameTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		value         any
		params        transformers.Parameters
		generatorType transformers.GeneratorType

		wantName string
		wantErr  error
	}{
		{
			name:  "ok - random with string",
			value: "alice",
			params: map[string]any{
				"gender": "female",
			},
			generatorType: transformers.Random,

			wantErr: nil,
		},
		{
			name:  "ok - random with []byte",
			value: []byte("alice"),
			params: map[string]any{
				"gender": "male",
			},
			generatorType: transformers.Random,

			wantErr: nil,
		},
		{
			name:  "ok - deterministic",
			value: "alice",
			params: map[string]any{
				"gender": "female",
			},
			generatorType: transformers.Deterministic,

			wantName: "Pearlie",
			wantErr:  nil,
		},
		{
			name:  "error - unsupported value type",
			value: 1,
			params: map[string]any{
				"gender": "female",
			},
			generatorType: transformers.Random,

			wantErr: transformers.ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewFirstNameTransformer(tc.generatorType, tc.params)
			require.NoError(t, err)

			got, err := transformer.Transform(tc.value)
			require.ErrorIs(t, err, tc.wantErr)
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
