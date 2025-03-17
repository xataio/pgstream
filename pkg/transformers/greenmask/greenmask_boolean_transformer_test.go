// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewBooleanTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		generator GeneratorType
		wantErr   error
	}{
		{
			name:      "ok - valid random",
			generator: Random,
			wantErr:   nil,
		},
		{
			name:      "ok - valid deterministic",
			generator: Deterministic,
			wantErr:   nil,
		},
		{
			name:      "error - invalid generator type",
			generator: "invalid",
			wantErr:   transformers.ErrUnsupportedGenerator,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewBooleanTransformer(tc.generator)
			require.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, transformer)
		})
	}
}

func Test_BooleanTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		generatorType GeneratorType
		input         any
		wantErr       error
	}{
		{
			name:          "ok - bool, random",
			generatorType: Random,
			input:         true,
			wantErr:       nil,
		},
		{
			name:          "ok - bool, deterministic",
			generatorType: Deterministic,
			input:         false,
			wantErr:       nil,
		},
		{
			name:          "ok - []byte, deterministic",
			generatorType: Deterministic,
			input:         []byte("123e4567-e89b-12d3-a456-426655440000"),
			wantErr:       nil,
		},
		{
			name:    "error - invalid input type",
			input:   "invalid",
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewBooleanTransformer(tc.generatorType)
			require.NoError(t, err)
			require.NotNil(t, transformer)

			got, err := transformer.Transform(tc.input)
			require.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, got)
			_, ok := got.(bool)
			require.True(t, ok)

			// if deterministic, the same input should always produce the same output
			if tc.generatorType == Deterministic {
				gotAgain, err := transformer.Transform(tc.input)
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
