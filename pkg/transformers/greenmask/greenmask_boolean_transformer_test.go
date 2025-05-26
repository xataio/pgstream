// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewBooleanTransformer(t *testing.T) {
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
			},
			wantErr: nil,
		},
		{
			name: "ok - valid deterministic",
			params: transformers.ParameterValues{
				"generator": deterministic,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid generator type",
			params: transformers.ParameterValues{
				"generator": "invalid",
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewBooleanTransformer(tc.params)
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
		name    string
		params  transformers.ParameterValues
		input   any
		wantErr error
	}{
		{
			name: "ok - bool, random",
			params: transformers.ParameterValues{
				"generator": random,
			},
			input:   true,
			wantErr: nil,
		},
		{
			name: "ok - bool, deterministic",
			params: transformers.ParameterValues{
				"generator": deterministic,
			},
			input:   false,
			wantErr: nil,
		},
		{
			name: "ok - []byte, deterministic",
			params: transformers.ParameterValues{
				"generator": deterministic,
			},
			input:   []byte("123e4567-e89b-12d3-a456-426655440000"),
			wantErr: nil,
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
			transformer, err := NewBooleanTransformer(tc.params)
			require.NoError(t, err)
			require.NotNil(t, transformer)

			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tc.input})
			require.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, got)
			_, ok := got.(bool)
			require.True(t, ok)

			// if deterministic, the same input should always produce the same output
			if mustGetGeneratorType(t, tc.params) == deterministic {
				gotAgain, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tc.input})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
