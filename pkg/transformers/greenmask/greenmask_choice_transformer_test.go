// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"testing"

	"github.com/eminano/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewChoiceTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.ParameterValues
		wantErr error
	}{
		{
			name: "ok - valid random",
			params: transformers.ParameterValues{
				"generator": random,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name: "error - invalid generator type",
			params: transformers.ParameterValues{
				"generator": "invalid",
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
		{
			name: "error - invalid choices",
			params: transformers.ParameterValues{
				"generator": deterministic,
			},
			wantErr: errChoicesEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewChoiceTransformer(tt.params)
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
		name    string
		input   any
		params  transformers.ParameterValues
		wantErr error
	}{
		{
			name:  "ok - transform string randomly",
			input: "test",
			params: transformers.ParameterValues{
				"generator": random,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:  "ok - transform []byte deterministically",
			input: []byte("test"),
			params: transformers.ParameterValues{
				"generator": deterministic,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:  "ok - transform RawValue deterministically",
			input: toolkit.NewRawValue([]byte("test"), false),
			params: transformers.ParameterValues{
				"generator": deterministic,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:  "error - invalid input type",
			input: 1,
			params: transformers.ParameterValues{
				"generator": random,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewChoiceTransformer(tt.params)
			require.NoError(t, err)
			require.NotNil(t, transformer)
			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
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
			if mustGetGeneratorType(t, tt.params) == deterministic {
				gotAgain, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
