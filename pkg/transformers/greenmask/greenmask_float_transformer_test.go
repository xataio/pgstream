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
		generatorType transformers.GeneratorType
		wantErr       error
	}{
		{
			name: "ok - random with parameters",
			params: map[string]any{
				"min_value": 1.5,
				"max_value": 10.5,
			},
			generatorType: transformers.Random,
			wantErr:       nil,
		},
		{
			name: "ok - deterministic with parameters",
			params: map[string]any{
				"min_value": 0.0,
				"max_value": 100.0,
				"precision": 4,
			},
			generatorType: transformers.Deterministic,
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
			generatorType: transformers.Deterministic,
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
			generatorType: transformers.Random,
			wantErr:       transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max_value type",
			params: map[string]any{
				"min_value": 1.5,
				"max_value": "invalid",
				"precision": 2,
			},
			generatorType: transformers.Deterministic,
			wantErr:       transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid precision type",
			params: map[string]any{
				"min_value": 1.5,
				"precision": "invalid",
			},
			generatorType: transformers.Random,
			wantErr:       transformers.ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewFloatTransformer(tc.generatorType, tc.params)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestFloatTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		value               any
		params              transformers.Parameters
		generatorType       transformers.GeneratorType
		wantErr             error
		deterministicResult float64
	}{
		{
			name:  "ok - random with float64",
			value: 5.5,
			params: map[string]any{
				"min_value": 1.0,
				"max_value": 10.0,
				"precision": 4,
			},
			generatorType: transformers.Random,
			wantErr:       nil,
		},
		{
			name:  "ok - random with float32",
			value: float32(5555.5),
			params: map[string]any{
				"min_value": 1.0,
				"max_value": 10.0,
			},
			generatorType: transformers.Random,
			wantErr:       nil,
		},
		{
			name:  "ok - deterministic with byte slice",
			value: []byte{0, 0, 0, 50},
			params: map[string]any{
				"min_value": 1.0,
				"max_value": 10.0,
			},
			generatorType:       transformers.Deterministic,
			wantErr:             nil,
			deterministicResult: 8.27,
		},
		{
			name:  "error - invalid value type",
			value: "invalid",
			params: map[string]any{
				"min_value": 1.0,
				"max_value": 10.0,
				"precision": 2,
			},
			generatorType: transformers.Random,
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
			generatorType: transformers.Random,
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

			minValue, err := findParameter(tc.params, "min_value", defaultMinFloat)
			require.NoError(t, err)
			maxValue, err := findParameter(tc.params, "max_value", defaultMaxFloat)
			require.NoError(t, err)

			switch v := got.(type) {
			case float64:
				require.GreaterOrEqual(t, v, minValue)
				require.LessOrEqual(t, v, maxValue)
			default:
				t.Errorf("unexpected type: %T", v)
			}

			if tc.generatorType == transformers.Deterministic {
				require.Equal(t, tc.deterministicResult, got)
			}
		})
	}
}
