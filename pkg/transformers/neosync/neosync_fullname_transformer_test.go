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
			name: "ok - length 3",
			params: transformers.ParameterValues{
				"max_length": 3,
				"seed":       21,
			},
			input:    "alice bob",
			wantErr:  nil,
			wantName: "A D",
		},
		{
			name: "ok - max_length 4",
			params: transformers.ParameterValues{
				"max_length": 4,
				"seed":       213,
			},
			input:    "alice bob",
			wantErr:  nil,
			wantName: "F L",
		},
		{
			name: "ok - max_length 4 again",
			params: transformers.ParameterValues{
				"max_length": 4,
				"seed":       21,
			},
			input:    "alice bob",
			wantErr:  nil,
			wantName: "Am D",
		},
		{
			name: "ok - preserve_length with last name length 1",
			params: transformers.ParameterValues{
				"preserve_length": true,
				"seed":            21,
			},
			input:    "mehmet y",
			wantErr:  nil,
			wantName: "Tanice M",
		},
		{
			name: "ok - preserve_length with no last name",
			params: transformers.ParameterValues{
				"preserve_length": true,
				"seed":            1234,
			},
			input:    "mehmet",
			wantErr:  nil,
			wantName: "Sotiri",
		},
		{
			name: "ok - preserve with max length 3",
			params: transformers.ParameterValues{
				"preserve_length": true,
				"seed":            21,
				"max_length":      3,
			},
			input:    "longname longsurname",
			wantErr:  nil,
			wantName: "Aleksejs De Bruycker",
		},
		{
			name: "ok - preserve with input length 3",
			params: transformers.ParameterValues{
				"preserve_length": true,
				"seed":            21,
				"max_length":      3,
			},
			input:    "A A",
			wantErr:  nil,
			wantName: "A D",
		},
		{
			name: "ok - preserve with input length 0",
			params: transformers.ParameterValues{
				"preserve_length": true,
				"seed":            21,
				"max_length":      3,
			},
			input:    "",
			wantErr:  nil,
			wantName: "",
		},
		{
			name: "error - max_length 1",
			params: transformers.ParameterValues{
				"max_length": 1,
				"seed":       21,
			},
			input:   "alice bob",
			wantErr: errFullNameLengthMustBeGreaterThanTwo,
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
			got, err := lst.Transform(context.Background(), transformers.Value{TransformValue: tc.input})
			require.NoError(t, err)
			require.Equal(t, tc.wantName, got)
		})
	}
}
