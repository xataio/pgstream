// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewFirstNameTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		params        transformers.ParameterValues
		dynamicParams transformers.ParameterValues

		wantErr error
	}{
		{
			name: "ok - random",
			params: map[string]any{
				"generator": random,
				"gender":    "female",
			},

			wantErr: nil,
		},
		{
			name: "ok - deterministic",
			params: map[string]any{
				"generator": deterministic,
				"gender":    "male",
			},

			wantErr: nil,
		},
		{
			name: "ok - unknown gender defaults to any",
			params: map[string]any{
				"generator": deterministic,
				"gender":    "other",
			},

			wantErr: nil,
		},
		{
			name: "error - invalid gender",
			params: map[string]any{
				"generator": random,
				"gender":    1,
			},

			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid generator",
			params: map[string]any{
				"generator": "invalid",
			},

			wantErr: transformers.ErrUnsupportedGenerator,
		},
		{
			name:   "error - parsing dynamic parameters",
			params: map[string]any{},
			dynamicParams: map[string]any{
				"gender": map[string]any{
					"invalid": "blob",
				},
			},

			wantErr: transformers.ErrInvalidDynamicParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewFirstNameTransformer(tc.params, tc.dynamicParams)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestFirstNameTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		value         any
		dynamicValues map[string]any
		params        transformers.ParameterValues
		dynamicParams transformers.ParameterValues

		wantName string
		wantErr  error
	}{
		{
			name:  "ok - random with string",
			value: "alice",
			params: map[string]any{
				"generator": random,
				"gender":    "female",
			},

			wantErr: nil,
		},
		{
			name:  "ok - random with []byte",
			value: []byte("alice"),
			params: map[string]any{
				"generator": random,
				"gender":    "male",
			},

			wantErr: nil,
		},
		{
			name:  "ok - deterministic",
			value: "alice",
			params: map[string]any{
				"generator": deterministic,
				"gender":    "female",
			},

			wantName: "Pearlie",
			wantErr:  nil,
		},
		{
			name:  "ok - deterministic with dynamic parameter",
			value: "bob",
			dynamicValues: map[string]any{
				"sex": "Male",
			},
			params: map[string]any{
				"generator": deterministic,
				"gender":    "female",
			},
			dynamicParams: map[string]any{
				"gender": map[string]any{
					"column": "sex",
				},
			},

			wantName: "Domingo",
			wantErr:  nil,
		},
		{
			name:  "error - invalid dynamic value type",
			value: "bob",
			dynamicValues: map[string]any{
				"sex": 1,
			},
			params: map[string]any{
				"generator": deterministic,
				"gender":    "female",
			},
			dynamicParams: map[string]any{
				"gender": map[string]any{
					"column": "sex",
				},
			},

			wantErr: errors.New("invalid value type for dynamic parameter \"gender\""),
		},
		{
			name:  "error - invalid dynamic value",
			value: "bob",
			dynamicValues: map[string]any{
				"sex": "Banana",
			},
			params: map[string]any{
				"generator": deterministic,
				"gender":    "female",
			},
			dynamicParams: map[string]any{
				"gender": map[string]any{
					"column": "sex",
				},
			},

			wantErr: errors.New("unable to match gender \"Banana\""),
		},
		{
			name:  "error - unsupported value type",
			value: 1,
			params: map[string]any{
				"generator": random,
				"gender":    "female",
			},

			wantErr: transformers.ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewFirstNameTransformer(tc.params, tc.dynamicParams)
			require.NoError(t, err)

			got, err := transformer.Transform(context.Background(), transformers.NewValue(tc.value, "text", tc.dynamicValues))
			require.Equal(t, err, tc.wantErr)
			if err != nil {
				return
			}

			if tc.wantName == "" {
				return
			}
			require.Equal(t, tc.wantName, got)
		})
	}
}
