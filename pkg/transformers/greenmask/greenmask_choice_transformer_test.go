// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"testing"

	"github.com/eminano/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewChoiceTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		generator transformers.GeneratorType
		params    transformers.Parameters
		wantErr   error
	}{
		{
			name:      "ok - valid random",
			generator: transformers.Random,
			params: transformers.Parameters{
				"choices": []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:      "error - invalid generator type",
			generator: "invalid",
			params: transformers.Parameters{
				"choices": []string{"a", "b", "c", "d"},
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
		{
			name:      "error - invalid choices",
			generator: transformers.Deterministic,
			wantErr:   errChoicesEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewChoiceTransformer(tt.generator, tt.params)
			require.Equal(t, tt.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, transformer)
		})
	}
}

func TestChoiceTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		generatorType transformers.GeneratorType
		input         any
		params        transformers.Parameters
		wantErr       error
	}{
		{
			name:          "ok - transform string randomly",
			generatorType: transformers.Random,
			input:         "test",
			params: transformers.Parameters{
				"choices": []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:          "ok - transform []byte deterministically",
			generatorType: transformers.Deterministic,
			input:         []byte("test"),
			params: transformers.Parameters{
				"choices": []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:          "ok - transform RawValue deterministically",
			generatorType: transformers.Deterministic,
			input:         toolkit.NewRawValue([]byte("test"), false),
			params: transformers.Parameters{
				"choices": []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:          "error - invalid input type",
			generatorType: transformers.Random,
			input:         1,
			params: transformers.Parameters{
				"choices": []string{"a", "b", "c", "d"},
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewChoiceTransformer(tt.generatorType, tt.params)
			require.NoError(t, err)
			require.NotNil(t, transformer)
			got, err := transformer.Transform(tt.input)
			require.Equal(t, tt.wantErr, err)
			if err != nil {
				return
			}

			// check if the result is in the choices
			require.NotNil(t, got)
			val, ok := got.([]byte)
			require.True(t, ok)
			require.Contains(t, tt.params["choices"], string(val))

			// if deterministic, check if we get the same result again
			if tt.generatorType == transformers.Deterministic {
				gotAgain, err := transformer.Transform(tt.input)
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
