// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"math"
	"testing"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewIntegerTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.Parameters
		wantErr error
	}{
		{
			name: "ok - valid default parameters",
			params: transformers.Parameters{
				"generator": random,
			},
			wantErr: nil,
		},
		{
			name: "ok - valid custom parameters",
			params: transformers.Parameters{
				"generator": deterministic,
				"size":      4,
				"min_value": -100,
				"max_value": 100,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid generator type",
			params: transformers.Parameters{
				"generator": "invalid",
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
		{
			name: "error - invalid size, too small",
			params: transformers.Parameters{
				"generator": random,
				"size":      0,
			},
			wantErr: errUnsupportedSizeError,
		},
		{
			name: "error - invalid size, too large",
			params: transformers.Parameters{
				"generator": deterministic,
				"size":      9,
			},
			wantErr: errUnsupportedSizeError,
		},
		{
			name: "error - wrong limits",
			params: transformers.Parameters{
				"generator": random,
				"min_value": 100,
				"max_value": 99,
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name: "error - wrong limits not fitting size",
			params: transformers.Parameters{
				"generator": random,
				"min_value": math.MaxInt,
				"size":      2,
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name: "error - invalid size type",
			params: transformers.Parameters{
				"generator": deterministic,
				"size":      "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid min_value type",
			params: transformers.Parameters{
				"generator": random,
				"min_value": "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max_value type",
			params: transformers.Parameters{
				"generator": deterministic,
				"max_value": "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewIntegerTransformer(tt.params)
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}

			require.NotNil(t, transformer)
		})
	}
}

func TestIntegerTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		input  any
		params transformers.Parameters

		wantErr error
	}{
		{
			name:  "ok - transform int8 randomly",
			input: int8(120),
			params: map[string]any{
				"generator": random,
				"size":      4,
				"min_value": -100,
				"max_value": 100,
			},
		},
		{
			name:  "ok - transform uint8 randomly",
			input: uint8(120),
			params: map[string]any{
				"generator": random,
				"size":      2,
				"min_value": -100,
				"max_value": 100,
			},
		},
		{
			name:  "ok - transform []byte randomly",
			input: []byte{0, 0, 0, 50},
			params: map[string]any{
				"generator": random,
				"size":      4,
				"min_value": -100,
				"max_value": 500,
			},
		},
		{
			name:  "ok - transform int16 randomly",
			input: int16(500),
			params: map[string]any{
				"generator": random,
				"min_value": -400,
				"max_value": 100,
			},
		},
		{
			name: "ok - transform int64 randomly with default params",
			params: transformers.Parameters{
				"generator": random,
			},
			input: int64(500),
		},
		{
			name: "ok - transform int deterministically with default params",
			params: transformers.Parameters{
				"generator": deterministic,
			},
			input: int(500),
		},
		{
			name: "ok - transform uint deterministically with default params",
			params: transformers.Parameters{
				"generator": deterministic,
			},
			input: uint(4646),
		},
		{
			name: "ok - transform uint32 deterministically with default params",
			params: transformers.Parameters{
				"generator": deterministic,
			},
			input: uint32(500000000),
		},
		{
			name:  "ok - transform int32 deterministically",
			input: int32(45000),
			params: map[string]any{
				"generator": deterministic,
				"size":      2,
				"min_value": -100,
			},
		},
		{
			name:  "ok - transform uint16 deterministically",
			input: uint16(0),
			params: map[string]any{
				"generator": deterministic,
				"size":      2,
				"min_value": -100,
			},
		},
		{
			name:  "ok - transform uint64 deterministically",
			input: uint64(1000000000000000000),
			params: map[string]any{
				"generator": deterministic,
				"size":      4,
				"min_value": math.MaxInt32 - 2,
			},
		},
		{
			name:  "ok - transform []byte deterministically, oversize",
			input: []byte{0, 1, 2, 3, 0, 0, 50, 0, 0, 0},
			params: map[string]any{
				"generator": deterministic,
				"size":      2,
				"min_value": -100,
			},
		},
		{
			name:  "invalid type with default params",
			input: "invalid",
			params: map[string]any{
				"generator": deterministic,
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
		{
			name:  "invalid type",
			input: "invalid",
			params: map[string]any{
				"generator": random,
				"size":      4,
				"min_value": -100,
				"max_value": 100,
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewIntegerTransformer(tt.params)
			require.NoError(t, err)

			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}

			result64, ok := got.(int64)
			require.True(t, ok, "expected got to be of type int64")
			result := int(result64)

			// check if the result is within the specified range
			minVal, found, err := transformers.FindParameter[int](tt.params, "min_value")
			require.NoError(t, err)
			if found {
				require.True(t, result >= minVal)
			}

			maxVal, found, err := transformers.FindParameter[int](tt.params, "max_value")
			require.NoError(t, err)
			if found {
				require.True(t, result <= maxVal)
			}

			// if deterministic, check if we get the same result again
			if mustGetGeneratorType(t, tt.params) == deterministic {
				gotAgain, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
