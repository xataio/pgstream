// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"testing"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewUnixTimestampTransformer(t *testing.T) {
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
				"min_value": int64(-2734919135000),
				"max_value": int64(72438947289300),
			},
			wantErr: nil,
		},
		{
			name:      "error - invalid min_value",
			generator: transformers.Deterministic,
			params: transformers.Parameters{
				"min_value": 3.0,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name:      "error - invalid max_value",
			generator: transformers.Deterministic,
			params: transformers.Parameters{
				"min_value": int64(1380002000000),
				"max_value": "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name:      "error - min_value missing",
			generator: transformers.Deterministic,
			params: transformers.Parameters{
				"max_value": int64(72438947289300),
			},
			wantErr: errMinMaxValueNotSpecified,
		},
		{
			name:      "error - invalid limits",
			generator: transformers.Random,
			params: transformers.Parameters{
				"min_value": int64(72438947289300),
				"max_value": int64(-72438947289300),
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUnixTimestampTransformer(tt.generator, tt.params)
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}

			require.NotNil(t, transformer)
		})
	}
}

func TestUnixTimestampTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		generator transformers.GeneratorType
		input     any
		params    transformers.Parameters
	}{
		{
			name:      "ok - random",
			generator: transformers.Random,
			params: transformers.Parameters{
				"min_value": int64(1625097600),
				"max_value": int64(1625184000),
			},
			input: int64(0),
		},
		{
			name:      "ok - deterministic",
			generator: transformers.Deterministic,
			params: transformers.Parameters{
				"min_value": int64(1625097600),
				"max_value": int64(1625184000),
			},
			input: int64(-1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUnixTimestampTransformer(tt.generator, tt.params)
			require.NoError(t, err)
			got, err := transformer.Transform(tt.input)
			require.NoError(t, err)
			require.NotNil(t, got)

			v, ok := got.(int64)
			require.True(t, ok)
			minVal, ok := tt.params["min_value"].(int64)
			require.True(t, ok)
			maxVal, ok := tt.params["max_value"].(int64)
			require.True(t, ok)
			require.GreaterOrEqual(t, v, minVal)
			require.LessOrEqual(t, v, maxVal)

			// if deterministic, check if we get the same result again
			if tt.generator == transformers.Deterministic {
				gotAgain, err := transformer.Transform(tt.input)
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
