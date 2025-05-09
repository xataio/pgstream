// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"strconv"
	"testing"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewUnixTimestampTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.Parameters
		wantErr error
	}{
		{
			name: "ok - valid random",
			params: transformers.Parameters{
				"generator": random,
				"min_value": "-1741957250",
				"max_value": "1741957250",
			},
			wantErr: nil,
		},
		{
			name: "error - invalid generator",
			params: transformers.Parameters{
				"generator": "invalid",
				"min_value": "-1741957250",
				"max_value": "1741957250",
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
		{
			name: "error - invalid min_value",
			params: transformers.Parameters{
				"generator": deterministic,
				"min_value": 3.0,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max_value",
			params: transformers.Parameters{
				"generator": deterministic,
				"min_value": "1741957250",
				"max_value": 3,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - min_value missing",
			params: transformers.Parameters{
				"generator": deterministic,
				"max_value": "1741957250",
			},
			wantErr: errMinMaxValueNotSpecified,
		},
		{
			name: "error - invalid limits",
			params: transformers.Parameters{
				"generator": random,
				"min_value": "1741957250",
				"max_value": "1741957250",
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUnixTimestampTransformer(tt.params)
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
		name   string
		input  any
		params transformers.Parameters
	}{
		{
			name: "ok - random",
			params: transformers.Parameters{
				"generator": random,
				"min_value": "1625097600",
				"max_value": "1625184000",
			},
			input: int64(0),
		},
		{
			name: "ok - deterministic",
			params: transformers.Parameters{
				"generator": deterministic,
				"min_value": "1625097600",
				"max_value": "1625184000",
			},
			input: int64(-1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUnixTimestampTransformer(tt.params)
			require.NoError(t, err)
			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
			require.NoError(t, err)
			require.NotNil(t, got)

			v, ok := got.(int64)
			require.True(t, ok)
			minValStr, ok := tt.params["min_value"].(string)
			require.True(t, ok)
			minVal, err := strconv.ParseInt(minValStr, 10, 64)
			require.NoError(t, err)
			maxValStr, ok := tt.params["max_value"].(string)
			require.True(t, ok)
			maxVal, err := strconv.ParseInt(maxValStr, 10, 64)
			require.NoError(t, err)
			require.GreaterOrEqual(t, v, minVal)
			require.LessOrEqual(t, v, maxVal)

			// if deterministic, check if we get the same result again
			if mustGetGeneratorType(t, tt.params) == deterministic {
				gotAgain, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}

func mustGetGeneratorType(t *testing.T, params transformers.Parameters) string {
	gt, err := getGeneratorType(params)
	require.NoError(t, err)
	return gt
}
