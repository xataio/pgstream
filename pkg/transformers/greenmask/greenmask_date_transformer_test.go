// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"testing"
	"time"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewDateTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		generator GeneratorType
		params    transformers.Parameters
		wantErr   error
	}{
		{
			name:      "ok - valid parameters",
			generator: Random,
			params: transformers.Parameters{
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			wantErr: nil,
		},
		{
			name:      "error - min_value missing",
			generator: Random,
			params: transformers.Parameters{
				"max_value": "2022-01-02",
			},
			wantErr: errMinMaxValueNotSpecified,
		},
		{
			name:      "error - min_value after max_value",
			generator: Random,
			params: transformers.Parameters{
				"min_value": "2022-01-03",
				"max_value": "2022-01-02",
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name:      "error - invalid generator type",
			generator: "invalid",
			params: transformers.Parameters{
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewDateTransformer(tt.generator, tt.params)
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, transformer)
		})
	}
}

func TestDateTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		generator GeneratorType
		params    transformers.Parameters
		input     any
		wantErr   error
	}{
		{
			name:      "ok - valid random",
			generator: Random,
			params: transformers.Parameters{
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			input:   "2021-01-01",
			wantErr: nil,
		},
		{
			name:      "ok - valid deterministic",
			generator: Deterministic,
			params: transformers.Parameters{
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			input:   []byte("2021-01-01"),
			wantErr: nil,
		},
		{
			name:      "ok - valid with time input",
			generator: Deterministic,
			params: transformers.Parameters{
				"min_value": "2022-01-02",
				"max_value": "2022-01-02",
			},
			input:   time.Now(),
			wantErr: nil,
		},
		{
			name:      "error - invalid input",
			generator: Random,
			params: transformers.Parameters{
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			input:   3,
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewDateTransformer(tt.generator, tt.params)
			require.NoError(t, err)
			require.NotNil(t, transformer)

			got, err := transformer.Transform(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}

			require.NotNil(t, got)
			result, ok := got.(time.Time)
			require.True(t, ok)

			minValue, _, _ := transformers.FindParameter[string](tt.params, "min_value")
			minDate, _ := time.Parse(time.DateOnly, minValue)

			maxValue, _, _ := transformers.FindParameter[string](tt.params, "max_value")
			maxDate, _ := time.Parse(time.DateOnly, maxValue)

			require.True(t, result.After(minDate) || result.Equal(minDate))
			require.True(t, result.Before(maxDate) || result.Equal(maxDate))

			if tt.generator == Deterministic {
				gotAgain, err := transformer.Transform(tt.input)
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
