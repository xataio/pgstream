// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"math"
	"math/big"
	"testing"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewFloatTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		params  transformers.ParameterValues
		wantErr error
	}{
		{
			name: "ok - random with parameters",
			params: map[string]any{
				"generator": random,
				"min_value": 1.01,
				"max_value": 10.5,
			},
			wantErr: nil,
		},
		{
			name: "ok - deterministic with parameters",
			params: map[string]any{
				"generator": deterministic,
				"min_value": 0.0000000000000000000000000000001,
				"max_value": 100.0,
				"precision": 44,
			},
			wantErr: nil,
		},
		{
			name: "ok - random with default",
			params: map[string]any{
				"generator": random,
			},
			wantErr: nil,
		},
		{
			name: "ok - deterministic with default",
			params: map[string]any{
				"generator": deterministic,
			},
			wantErr: nil,
		},
		{
			name: "error - min_value greater than max_value",
			params: map[string]any{
				"generator": random,
				"min_value": 10.5,
				"max_value": 1.5,
				"precision": 2,
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name: "error - invalid min_value type",
			params: map[string]any{
				"generator": random,
				"min_value": "invalid",
				"max_value": 10.5,
				"precision": 2,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max_value type",
			params: map[string]any{
				"generator": deterministic,
				"min_value": 1.5,
				"max_value": "invalid",
				"precision": 2,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid precision type",
			params: map[string]any{
				"generator": random,
				"min_value": 1.5,
				"precision": "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid generator type",
			params: map[string]any{
				"generator": "invalid",
				"min_value": 1.5,
				"max_value": 10.5,
				"precision": 2,
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewFloatTransformer(tc.params)
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
		name    string
		value   any
		params  transformers.ParameterValues
		wantErr error
	}{
		{
			name:  "ok - random with float64",
			value: float64(5.5),
			params: map[string]any{
				"generator": random,
				"min_value": 9.999999999999,
				"max_value": 10.0,
				"precision": 12,
			},
			wantErr: nil,
		},
		{
			name:  "ok - deterministic with float32, with default params",
			value: float32(5555.5),
			params: transformers.ParameterValues{
				"generator": deterministic,
			},
			wantErr: nil,
		},
		{
			name:  "ok - deterministic with byte slice",
			value: []byte{0, 0, 0, 50},
			params: map[string]any{
				"generator": deterministic,
				"min_value": 1.0,
				"max_value": 100000.0000000001,
			},
			wantErr: nil,
		},
		{
			name:  "ok - deterministic with numeric",
			value: pgtype.Numeric{Int: big.NewInt(51507351), Exp: -6, Valid: true},
			params: map[string]any{
				"generator": deterministic,
				"min_value": -90.0,
				"max_value": 90.0,
				"precision": 6,
			},
			wantErr: nil,
		},
		{
			name:  "ok - random with a numeric beyond float64 precision",
			value: pgtype.Numeric{Int: mustParseBigInt(t, "987654321123456789123456789"), Exp: -9, Valid: true},
			params: map[string]any{
				"generator": random,
				"min_value": 0.0,
				"max_value": 1000.0,
			},
			wantErr: nil,
		},
		{
			// clamped rather than failing: pgtype reports this as a strconv
			// range error, and erroring here would null the column
			name:  "ok - random with a numeric beyond float64 range",
			value: pgtype.Numeric{Int: big.NewInt(1), Exp: 400, Valid: true},
			params: map[string]any{
				"generator": random,
				"min_value": 1.0,
				"max_value": 10.0,
			},
			wantErr: nil,
		},
		{
			name:  "ok - random with a negative numeric beyond float64 range",
			value: pgtype.Numeric{Int: big.NewInt(-1), Exp: 400, Valid: true},
			params: map[string]any{
				"generator": random,
				"min_value": 1.0,
				"max_value": 10.0,
			},
			wantErr: nil,
		},
		{
			// pgx yields a zero float for an invalid numeric; the pipeline
			// skips nulls before this point, so it only has to not blow up
			name:  "ok - random with an invalid numeric",
			value: pgtype.Numeric{Valid: false},
			params: map[string]any{
				"generator": random,
				"min_value": 1.0,
				"max_value": 10.0,
			},
			wantErr: nil,
		},
		{
			// wal2json renders a whole numeric as a JSON integer and the
			// replication listener decodes it as int64
			name:  "ok - deterministic with int64 from the CDC path",
			value: int64(1234),
			params: map[string]any{
				"generator": deterministic,
				"min_value": 0.0,
				"max_value": 1000.0,
			},
			wantErr: nil,
		},
		{
			name:  "ok - deterministic with int from the CDC path",
			value: 1234,
			params: map[string]any{
				"generator": deterministic,
				"min_value": 0.0,
				"max_value": 1000.0,
			},
			wantErr: nil,
		},
		{
			name:  "ok - deterministic with a NaN numeric",
			value: pgtype.Numeric{NaN: true, Valid: true},
			params: map[string]any{
				"generator": deterministic,
				"min_value": 1.0,
				"max_value": 10.0,
			},
			wantErr: nil,
		},
		{
			name:  "error - invalid value type",
			value: "invalid",
			params: map[string]any{
				"generator": random,
				"min_value": 1.0,
				"max_value": 10.0,
				"precision": 2,
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
		{
			name:  "error - nil value",
			value: nil,
			params: map[string]any{
				"generator": random,
				"min_value": 1.0,
				"max_value": 10.0,
				"precision": 2,
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewFloatTransformer(tc.params)
			require.NoError(t, err)

			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tc.value})
			require.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}
			result, ok := got.(float64)
			require.True(t, ok, "expected got to be of type float64")
			require.False(t, math.IsNaN(result), "a transformed value must never be NaN")

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
			if mustGetGeneratorType(t, tc.params) == deterministic {
				gotAgain, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tc.value})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}

func mustParseBigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	i, ok := new(big.Int).SetString(s, 10)
	require.True(t, ok, "invalid big.Int literal %q", s)
	return i
}

// a numeric column reaches the transformer as a pgtype.Numeric, while the same
// value on a double precision column reaches it as a float64. Both have to
// seed the generator identically, or the same source value would anonymize
// differently depending on the column's declared type.
func TestFloatTransformer_Transform_numericMatchesFloat64(t *testing.T) {
	t.Parallel()

	params := transformers.ParameterValues{
		"generator": deterministic,
		"min_value": -90.0,
		"max_value": 90.0,
		"precision": 6,
	}

	transformer, err := NewFloatTransformer(params)
	require.NoError(t, err)

	fromNumeric, err := transformer.Transform(context.Background(), transformers.Value{
		TransformValue: pgtype.Numeric{Int: big.NewInt(51507351), Exp: -6, Valid: true},
	})
	require.NoError(t, err)

	fromFloat, err := transformer.Transform(context.Background(), transformers.Value{
		TransformValue: 51.507351,
	})
	require.NoError(t, err)

	require.Equal(t, fromFloat, fromNumeric)
}

// the same whole numeric reaches this transformer as a pgtype.Numeric from a
// snapshot and as an int64 from the replication stream, since wal2json renders
// it as a JSON integer. Both have to seed the generator identically, or a row
// would anonymize differently depending on which half of the pipeline carried
// it.
func TestFloatTransformer_Transform_int64MatchesNumeric(t *testing.T) {
	t.Parallel()

	transformer, err := NewFloatTransformer(transformers.ParameterValues{
		"generator": deterministic,
		"min_value": 0.0,
		"max_value": 1000.0,
	})
	require.NoError(t, err)

	fromNumeric, err := transformer.Transform(context.Background(), transformers.Value{
		TransformValue: pgtype.Numeric{Int: big.NewInt(1234), Exp: 0, Valid: true},
	})
	require.NoError(t, err)

	fromInt64, err := transformer.Transform(context.Background(), transformers.Value{
		TransformValue: int64(1234),
	})
	require.NoError(t, err)

	fromFloat, err := transformer.Transform(context.Background(), transformers.Value{
		TransformValue: float64(1234),
	})
	require.NoError(t, err)

	require.Equal(t, fromFloat, fromNumeric)
	require.Equal(t, fromFloat, fromInt64)
}
