// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"testing"
	"time"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewDateTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.Parameters
		wantErr error
	}{
		{
			name: "ok - valid parameters",
			params: transformers.Parameters{
				"generator": random,
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			wantErr: nil,
		},
		{
			name: "error - min_value missing",
			params: transformers.Parameters{
				"generator": random,
				"max_value": "2022-01-02",
			},
			wantErr: errMinMaxValueNotSpecified,
		},
		{
			name: "error - min_value after max_value",
			params: transformers.Parameters{
				"generator": random,
				"min_value": "2022-01-03",
				"max_value": "2022-01-02",
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name: "error - invalid generator type",
			params: transformers.Parameters{
				"generator": "invalid",
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewDateTransformer(tt.params)
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
		name    string
		params  transformers.Parameters
		input   any
		wantErr error
	}{
		{
			name: "ok - valid random",
			params: transformers.Parameters{
				"generator": random,
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			input:   "2021-01-01",
			wantErr: nil,
		},
		{
			name: "ok - valid deterministic",
			params: transformers.Parameters{
				"generator": deterministic,
				"min_value": "2021-01-01",
				"max_value": "2022-01-02",
			},
			input:   []byte("2021-01-01"),
			wantErr: nil,
		},
		{
			name: "ok - valid with time input",
			params: transformers.Parameters{
				"generator": deterministic,
				"min_value": "2022-01-02",
				"max_value": "2022-01-02",
			},
			input:   time.Now(),
			wantErr: nil,
		},
		{
			name: "error - invalid input",
			params: transformers.Parameters{
				"generator": random,
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
			transformer, err := NewDateTransformer(tt.params)
			require.NoError(t, err)
			require.NotNil(t, transformer)

			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
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

			if mustGetGeneratorType(t, tt.params) == deterministic {
				gotAgain, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
