// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"testing"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewFloatTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		params        transformers.Parameters
		generatorType GeneratorType
		wantErr       error
	}{
		{
			name: "ok - random with parameters",
			params: map[string]any{
				"min_value": 1.01,
				"max_value": 10.5,
			},
			generatorType: Random,
			wantErr:       nil,
		},
		{
			name: "ok - deterministic with parameters",
			params: map[string]any{
				"min_value": 0.0000000000000000000000000000001,
				"max_value": 100.0,
				"precision": 44,
			},
			generatorType: Deterministic,
			wantErr:       nil,
		},
		{
			name:    "ok - random with default",
			params:  map[string]any{},
			wantErr: nil,
		},
		{
			name:          "ok - deterministic with default",
			params:        map[string]any{},
			generatorType: Deterministic,
			wantErr:       nil,
		},
		{
			name: "error - min_value greater than max_value",
			params: map[string]any{
				"min_value": 10.5,
				"max_value": 1.5,
				"precision": 2,
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name: "error - invalid min_value type",
			params: map[string]any{
				"min_value": "invalid",
				"max_value": 10.5,
				"precision": 2,
			},
			generatorType: Random,
			wantErr:       transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max_value type",
			params: map[string]any{
				"min_value": 1.5,
				"max_value": "invalid",
				"precision": 2,
			},
			generatorType: Deterministic,
			wantErr:       transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid precision type",
			params: map[string]any{
				"min_value": 1.5,
				"precision": "invalid",
			},
			generatorType: Random,
			wantErr:       transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid generator type",
			params: map[string]any{
				"min_value": 1.5,
				"max_value": 10.5,
				"precision": 2,
			},
			generatorType: "invalid",
			wantErr:       transformers.ErrUnsupportedGenerator,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewFloatTransformer(tc.generatorType, tc.params)
			require.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, transformer)
		})
	}
}

func TestFloatTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		value         any
		params        transformers.Parameters
		generatorType GeneratorType
		wantErr       error
	}{
		{
			name:  "ok - random with float64",
			value: float64(5.5),
			params: map[string]any{
				"min_value": 9.999999999999,
				"max_value": 10.0,
				"precision": 12,
			},
			generatorType: Random,
			wantErr:       nil,
		},
		{
			name:          "ok - deterministic with float32, with default params",
			value:         float32(5555.5),
			generatorType: Deterministic,
			wantErr:       nil,
		},
		{
			name:  "ok - deterministic with byte slice",
			value: []byte{0, 0, 0, 50},
			params: map[string]any{
				"min_value": 1.0,
				"max_value": 100000.0000000001,
			},
			generatorType: Deterministic,
			wantErr:       nil,
		},
		{
			name:  "error - invalid value type",
			value: "invalid",
			params: map[string]any{
				"min_value": 1.0,
				"max_value": 10.0,
				"precision": 2,
			},
			generatorType: Random,
			wantErr:       transformers.ErrUnsupportedValueType,
		},
		{
			name:  "error - nil value",
			value: nil,
			params: map[string]any{
				"min_value": 1.0,
				"max_value": 10.0,
				"precision": 2,
			},
			generatorType: Random,
			wantErr:       transformers.ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewFloatTransformer(tc.generatorType, tc.params)
			require.NoError(t, err)

			got, err := transformer.Transform(tc.value)
			require.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}
			result, ok := got.(float64)
			require.True(t, ok, "expected got to be of type float64")

			// check if the result is within the specified range
			minVal, found, err := transformers.FindParameter[float64](tc.params, "min_value")
			require.NoError(t, err)
			if found {
				require.True(t, result >= minVal)
			}

			maxVal, found, err := transformers.FindParameter[float64](tc.params, "max_value")
			require.NoError(t, err)
			if found {
				require.True(t, result <= maxVal)
			}

			// if deterministic, check if we get the same result again
			if tc.generatorType == Deterministic {
				gotAgain, err := transformer.Transform(tc.value)
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
