// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewLastnameTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		params   transformers.ParameterValues
		input    any
		wantErr  error
		wantName string
	}{
		{
			name: "ok - valid",
			params: transformers.ParameterValues{
				"preserve_length": false,
				"max_length":      10,
				"seed":            123,
			},
			input:    "lastname",
			wantErr:  nil,
			wantName: "Fournaris",
		},
		{
			name:  "ok - length 1",
			input: "Liddell",
			params: map[string]any{
				"max_length": 1,
				"seed":       12,
			},

			wantName: "I",
			wantErr:  nil,
		},
		{
			name:  "error - length -1",
			input: "alice",
			params: map[string]any{
				"max_length": -1,
				"seed":       12,
			},

			wantErr: errLastNameLengthMustBeGreaterThanZero,
		},
		{
			name: "error - invalid preserve_length",
			params: transformers.ParameterValues{
				"preserve_length": 123,
				"max_length":      10,
				"seed":            123,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max_length",
			params: transformers.ParameterValues{
				"preserve_length": true,
				"max_length":      "invalid",
				"seed":            123,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid seed",
			params: transformers.ParameterValues{
				"preserve_length": true,
				"max_length":      10,
				"seed":            "invalid",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lst, err := NewLastNameTransformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}
			require.NoError(t, err)
			require.NotNil(t, lst)
			got, _ := lst.Transform(context.Background(), transformers.Value{TransformValue: tc.input})
			require.Equal(t, tc.wantName, got)
		})
	}
}
