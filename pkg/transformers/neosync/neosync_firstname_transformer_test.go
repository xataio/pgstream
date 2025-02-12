// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestFirstnameTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		value  any
		params transformers.Parameters

		wantName string
		wantErr  error
	}{
		{
			name:  "ok",
			value: "alice",
			params: map[string]any{
				"preserve_length": false,
				"max_length":      4,
				"seed":            0,
			},

			wantName: "Ute",
			wantErr:  nil,
		},
		{
			name:  "error - invalid preserve length",
			value: "alice",
			params: map[string]any{
				"preserve_length": 1,
			},

			wantName: "",
			wantErr:  transformers.ErrInvalidParameters,
		},
		{
			name:  "error - invalid max length",
			value: "alice",
			params: map[string]any{
				"max_length": "1",
			},

			wantName: "",
			wantErr:  transformers.ErrInvalidParameters,
		},
		{
			name:  "error - invalid seed",
			value: "alice",
			params: map[string]any{
				"seed": "1",
			},

			wantName: "",
			wantErr:  transformers.ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewFirstNameTransformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)

			if err != nil {
				return
			}

			got, err := transformer.Transform(tc.value)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantName, got)
		})
	}
}
