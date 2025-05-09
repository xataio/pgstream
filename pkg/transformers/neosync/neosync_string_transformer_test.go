// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestStringTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		value  any
		params transformers.Parameters

		wantString string
		wantErr    error
	}{
		{
			name:  "ok",
			value: "hello",
			params: map[string]any{
				"preserve_length": false,
				"min_length":      2,
				"max_length":      2,
				"seed":            0,
			},

			wantString: "np",
			wantErr:    nil,
		},
		{
			name:  "error - invalid preserve length",
			value: "hello",
			params: map[string]any{
				"preserve_length": 1,
			},

			wantString: "",
			wantErr:    transformers.ErrInvalidParameters,
		},
		{
			name:  "error - invalid min length",
			value: "hello",
			params: map[string]any{
				"min_length": "1",
			},

			wantString: "",
			wantErr:    transformers.ErrInvalidParameters,
		},
		{
			name:  "error - invalid max length",
			value: "hello",
			params: map[string]any{
				"max_length": "1",
			},

			wantString: "",
			wantErr:    transformers.ErrInvalidParameters,
		},
		{
			name:  "error - invalid seed",
			value: "hello",
			params: map[string]any{
				"seed": "1",
			},

			wantString: "",
			wantErr:    transformers.ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewStringTransformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)

			if err != nil {
				return
			}

			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tc.value})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantString, got)
		})
	}
}
