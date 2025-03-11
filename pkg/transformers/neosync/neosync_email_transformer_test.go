// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewEmailTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.Parameters
		wantErr error
	}{
		{
			name:    "ok - valid default parameters",
			params:  transformers.Parameters{},
			wantErr: nil,
		},
		{
			name: "ok - valid custom parameters",
			params: transformers.Parameters{
				"email_type":           "free_email",
				"invalid_email_action": "random",
				"excluded_domains":     []string{"example.com"},
				"max_length":           10,
				"preserve_domain":      true,
				"preserve_length":      true,
				"seed":                 0,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid preserve_length",
			params: transformers.Parameters{
				"preserve_length": 1,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid preserve_domain",
			params: transformers.Parameters{
				"preserve_domain": 1,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid excluded_domains",
			params: transformers.Parameters{
				"excluded_domains": 1,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid max_length",
			params: transformers.Parameters{
				"max_length": "1",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid seed",
			params: transformers.Parameters{
				"seed": "1",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid email_type",
			params: transformers.Parameters{
				"email_type": 1,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid invalid_email_action",
			params: transformers.Parameters{
				"invalid_email_action": 1,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewEmailTransformer(tt.params)
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, transformer)
		})
	}
}

func TestEmailTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		input  any
		params transformers.Parameters

		wantErr error
	}{
		{
			name:    "ok - valid default parameters",
			input:   "myname@lastname.com",
			params:  transformers.Parameters{},
			wantErr: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewEmailTransformer(tc.params)
			require.NoError(t, err)
			got, err := transformer.Transform(tc.input)
			require.ErrorIs(t, err, tc.wantErr)
			require.NotNil(t, got)
		})
	}
}
