// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewFullnameTransformer(t *testing.T) {
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
				"max_length":      20,
				"seed":            1234,
			},
			input:    "name surname",
			wantErr:  nil,
			wantName: "Flav Di Chiara",
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
			lst, err := NewFullNameTransformer(tc.params)
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
