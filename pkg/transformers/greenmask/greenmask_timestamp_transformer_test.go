// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"testing"
	"time"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewUTCTimestampTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.Parameters
		wantErr error
	}{
		{
			name: "ok - valid random",
			params: transformers.Parameters{
				"generator":     random,
				"min_timestamp": "2021-01-01T00:00:00Z",
				"max_timestamp": "2022-01-02T00:00:00Z",
			},
			wantErr: nil,
		},
		{
			name: "error - invalid truncate_part",
			params: transformers.Parameters{
				"generator":     deterministic,
				"truncate_part": 1.2,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid min_timestamp",
			params: transformers.Parameters{
				"generator":     deterministic,
				"min_timestamp": 1.2,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid min_timestamp format",
			params: transformers.Parameters{
				"generator":     deterministic,
				"min_timestamp": "2021 Jan 01",
				"max_timestamp": "2022-01-02T00:00:00Z",
			},
			wantErr: errInvalidTimestamp,
		},
		{
			name: "error - invalid max_timestamp",
			params: transformers.Parameters{
				"generator":     random,
				"min_timestamp": "2021-01-01T00:00:00Z",
				"max_timestamp": 1.2,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max_timestamp format",
			params: transformers.Parameters{
				"generator":     deterministic,
				"min_timestamp": "2021-01-01T00:00:00Z",
				"max_timestamp": "2021 Jan 01",
			},
			wantErr: errInvalidTimestamp,
		},
		{
			name: "error - min_timestamp not specified",
			params: transformers.Parameters{
				"generator":     random,
				"max_timestamp": "2022-01-02T00:00:00Z",
			},
			wantErr: errMinMaxTimestampNotSpecified,
		},
		{
			name: "error - min_timestamp equal to max_timestamp",
			params: transformers.Parameters{
				"generator":     deterministic,
				"min_timestamp": "2023-01-02T00:00:00Z",
				"max_timestamp": "2023-01-02T00:01:00+01:00",
			},
			wantErr: greenmasktransformers.ErrWrongLimits,
		},
		{
			name: "error - invalid generator type",
			params: transformers.Parameters{
				"generator":     "invalid",
				"min_timestamp": "2021-01-01T00:00:00Z",
				"max_timestamp": "2022-01-02T00:00:00Z",
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUTCTimestampTransformer(tt.params)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, transformer)
		})
	}
}

func TestUTCTimestampTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   any
		params  transformers.Parameters
		wantErr error
	}{
		{
			name:  "ok - transform string randomly",
			input: "test",
			params: transformers.Parameters{
				"generator":     random,
				"min_timestamp": "2022-01-01T00:00:00Z",
				"max_timestamp": "2023-01-02T00:00:00Z",
				"truncate_part": "month",
			},
			wantErr: nil,
		},
		{
			name:  "ok - transform []byte deterministically",
			input: []byte("test"),
			params: transformers.Parameters{
				"generator":     deterministic,
				"min_timestamp": "2021-01-01T00:00:00Z",
				"max_timestamp": "2023-01-02T00:00:00Z",
				"truncate_part": "day",
			},
			wantErr: nil,
		},
		{
			name:  "ok - transform time.Time deterministically",
			input: time.Now(),
			params: transformers.Parameters{
				"generator":     deterministic,
				"min_timestamp": "2020-01-01T00:00:00Z",
				"max_timestamp": "2023-01-02T00:00:00Z",
				"truncate_part": "hour",
			},
			wantErr: nil,
		},
		{
			name:  "ok - truncate after millisecond part",
			input: "2021-01-01T00:00:00Z",
			params: transformers.Parameters{
				"generator":     deterministic,
				"min_timestamp": "2023-01-01T00:00:00Z",
				"max_timestamp": "2023-01-01T01:10:00+01:00",
				"truncate_part": "millisecond",
			},
			wantErr: nil,
		},
		{
			name:  "error - invalid input type",
			input: 1,
			params: transformers.Parameters{
				"generator":     deterministic,
				"min_timestamp": "2020-01-01T00:00:00Z",
				"max_timestamp": "2023-01-02T00:00:00Z",
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUTCTimestampTransformer(tt.params)
			require.NoError(t, err)
			require.NotNil(t, transformer)

			got, err := transformer.Transform(transformers.Value{TransformValue: tt.input})
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, got)
			result, ok := got.(time.Time)
			require.True(t, ok, "expected got to be of type time.Time")

			minStr, _ := tt.params["min_timestamp"].(string)
			minTimestamp, _ := time.Parse(time.RFC3339, minStr)
			maxStr, _ := tt.params["max_timestamp"].(string)
			maxTimestamp, _ := time.Parse(time.RFC3339, maxStr)

			truncatePart, _, _ := transformers.FindParameter[string](tt.params, "truncate_part")

			// check if the result is truncated accordingly
			// truncate min and max timestamps as well, so we can compare them with the result
			switch truncatePart {
			case "year":
				require.Equal(t, int(result.Month()), 1)
				minTimestamp = time.Date(minTimestamp.Year(), 1, 1, 0, 0, 0, 0, minTimestamp.Location())
				maxTimestamp = time.Date(maxTimestamp.Year(), 1, 1, 0, 0, 0, 0, maxTimestamp.Location())
			case "month":
				require.Equal(t, result.Day(), 1)
				minTimestamp = time.Date(minTimestamp.Year(), minTimestamp.Month(), 1, 0, 0, 0, 0, minTimestamp.Location())
				maxTimestamp = time.Date(maxTimestamp.Year(), maxTimestamp.Month(), 1, 0, 0, 0, 0, maxTimestamp.Location())
			case "day":
				require.Equal(t, result.Hour(), 0)
				minTimestamp = minTimestamp.Truncate(24 * time.Hour)
				maxTimestamp = maxTimestamp.Truncate(24 * time.Hour)
			case "hour":
				require.Equal(t, result.Minute(), 0)
				minTimestamp = minTimestamp.Truncate(time.Hour)
				maxTimestamp = maxTimestamp.Truncate(time.Hour)
			case "minute":
				require.Equal(t, result.Second(), 0)
				minTimestamp = minTimestamp.Truncate(time.Minute)
				maxTimestamp = maxTimestamp.Truncate(time.Minute)
			case "second":
				require.Equal(t, result.Nanosecond(), 0)
				minTimestamp = minTimestamp.Truncate(time.Second)
				maxTimestamp = maxTimestamp.Truncate(time.Second)
			}

			require.Equal(t, result.Location(), time.UTC)

			require.True(t, result.After(minTimestamp) || result.Equal(minTimestamp))
			// require.True(t, result.Before(maxTimestamp) || result.Equal(maxTimestamp))
			require.LessOrEqual(t, result, maxTimestamp)

			// if deterministic, check that the same input always produces the same output
			if mustGetGeneratorType(t, tt.params) == deterministic {
				gotAgain, err := transformer.Transform(transformers.Value{TransformValue: tt.input})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
