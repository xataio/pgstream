// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"math"
	"testing"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewIntegerTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		generator transformers.GeneratorType
		params    transformers.Parameters
		wantErr   error
	}{
		{
			name:      "ok - valid default parameters",
			generator: transformers.Random,
			params:    transformers.Parameters{},
			wantErr:   nil,
		},
		{
			name:      "ok - valid custom parameters",
			generator: transformers.Deterministic,
			params: transformers.Parameters{
				"size":      4,
				"min_value": int64(-100),
				"max_value": int64(100),
			},
			wantErr: nil,
		},
		{
			name:      "error - invalid generator type",
			generator: "invalid",
			wantErr:   transformers.ErrUnsupportedGenerator,
		},
		{
			name:      "error - invalid size, too small",
			generator: transformers.Random,
			params: transformers.Parameters{
				"size": 0,
			},
			wantErr: errUnsupportedSizeError,
		},
		{
			name:      "error - invalid size, too large",
			generator: transformers.Deterministic,
			params: transformers.Parameters{
				"size": 9,
			},
			wantErr: errUnsupportedSizeError,
		},
		{
			name:      "error - wrong limits",
			generator: transformers.Random,
			params: transformers.Parameters{
				"min_value": int64(100),
				"max_value": int64(99),
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name:      "error - wrong limits not fitting size",
			generator: transformers.Random,
			params: transformers.Parameters{
				"min_value": int64(math.MaxInt32),
				"size":      4,
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name:      "error - invalid size type",
			generator: transformers.Deterministic,
			params: transformers.Parameters{
				"size": "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name:      "error - invalid min_value type",
			generator: transformers.Random,
			params: transformers.Parameters{
				"min_value": "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name:      "error - invalid max_value type",
			generator: transformers.Deterministic,
			params: transformers.Parameters{
				"max_value": "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewIntegerTransformer(tt.generator, tt.params)
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
		name          string
		generatorType transformers.GeneratorType
		input         any
		params        transformers.Parameters

		wantErr    error
		wantResult int64
	}{
		{
			name:          "ok - transform int8 randomly",
			generatorType: transformers.Random,
			input:         int8(120),
			params: map[string]any{
				"size":      8,
				"min_value": int64(-100),
				"max_value": int64(100),
			},
		},
		{
			name:          "ok - transform uint8 randomly",
			generatorType: transformers.Random,
			input:         uint8(120),
			params: map[string]any{
				"size":      2,
				"min_value": int64(-100),
				"max_value": int64(100),
			},
		},
		{
			name:          "ok - transform []byte randomly",
			generatorType: transformers.Random,
			input:         []byte{0, 0, 0, 50},
			params: map[string]any{
				"size":      4,
				"min_value": int64(-100),
				"max_value": int64(500),
			},
		},
		{
			name:          "ok - transform int16 randomly",
			generatorType: transformers.Random,
			input:         int16(500),
			params: map[string]any{
				"size":      8,
				"min_value": int64(-400),
				"max_value": int64(100),
			},
		},
		{
			name:          "ok - transform int64 randomly with default params",
			generatorType: transformers.Random,
			input:         int64(500),
		},
		{
			name:          "ok - transform []byte randomly with default params",
			generatorType: transformers.Random,
			input:         []byte{0, 0, 0, 50},
		},
		{
			name:          "ok - transform []byte randomly with default params, oversize",
			generatorType: transformers.Random,
			input:         []byte{0, 0, 0, 0, 0, 0, 0, 50},
		},
		{
			name:          "ok - transform int deterministically with default params",
			generatorType: transformers.Deterministic,
			input:         int(500),
			wantResult:    2035278536,
		},
		{
			name:          "ok - transform uint deterministically with default params",
			generatorType: transformers.Deterministic,
			input:         uint(4646),
			wantResult:    -1391990283,
		},
		{
			name:          "ok - transform uint32 deterministically with default params",
			generatorType: transformers.Deterministic,
			input:         uint32(500000000),
			params:        map[string]any{},
			wantResult:    -722347270,
		},
		{
			name:          "ok - transform []byte deterministically with default params",
			generatorType: transformers.Deterministic,
			input:         []byte{0, 0, 0, 50},
			params:        map[string]any{},
			wantResult:    612657282,
		},
		{
			name:          "ok - transform int32 deterministically",
			generatorType: transformers.Deterministic,
			input:         int32(45000),
			params: map[string]any{
				"size":      2,
				"min_value": int64(-100),
			},
			wantResult: 15232,
		},
		{
			name:          "ok - transform uint16 deterministically",
			generatorType: transformers.Deterministic,
			input:         uint16(0),
			params: map[string]any{
				"size":      2,
				"min_value": int64(-100),
			},
			wantResult: 23208,
		},
		{
			name:          "ok - transform uint64 deterministically",
			generatorType: transformers.Deterministic,
			input:         uint64(1000000000000000000),
			params: map[string]any{
				"size":      8,
				"min_value": int64(math.MaxInt64 - 1),
			},
			wantResult: int64(math.MaxInt64 - 1),
		},
		{
			name:          "ok - transform []byte deterministically, oversize",
			generatorType: transformers.Deterministic,
			input:         []byte{0, 1, 2, 3, 0, 0, 50, 0, 0, 0},
			params: map[string]any{
				"size":      2,
				"min_value": int64(-100),
			},
			wantResult: 18547,
		},
		{
			name:          "invalid type with deterministic with default params",
			generatorType: transformers.Deterministic,
			input:         "invalid",
			params:        map[string]any{},
			wantErr:       transformers.ErrUnsupportedValueType,
		},
		{
			name:          "invalid type",
			generatorType: transformers.Random,
			input:         "invalid",
			params: map[string]any{
				"size":      8,
				"min_value": int64(-100),
				"max_value": int64(100),
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
		{
			name:          "invalid type with default params",
			generatorType: transformers.Random,
			input:         "invalid",
			wantErr:       transformers.ErrUnsupportedValueType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewIntegerTransformer(tt.generatorType, tt.params)
			require.NoError(t, err)

			got, err := transformer.Transform(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}

			result, ok := got.(int64)
			require.True(t, ok, "expected got to be of type int64")

			if tt.generatorType == transformers.Deterministic {
				require.Equal(t, tt.wantResult, result)
				return
			}

			// check if the result is within the specified range
			minVal, found, err := transformers.FindParameter[int64](tt.params, "min_value")
			require.NoError(t, err)
			if found {
				require.True(t, result >= minVal)
			}

			maxVal, found, err := transformers.FindParameter[int64](tt.params, "max_value")
			require.NoError(t, err)
			if found {
				require.True(t, result <= maxVal)
			}
		})
	}
}
