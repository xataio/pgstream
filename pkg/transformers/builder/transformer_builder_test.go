// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestTransformerBuilder_New(t *testing.T) {
	tests := []struct {
		name    string
		config  *transformers.Config
		wantErr error
	}{
		{
			name: "valid greenmask string transformer",
			config: &transformers.Config{
				Name:       transformers.GreenmaskString,
				Parameters: map[string]any{"max_length": 10},
			},
			wantErr: nil,
		},
		{
			name: "valid email transformer",
			config: &transformers.Config{
				Name:       transformers.Email,
				Parameters: nil,
			},
			wantErr: nil,
		},
		{
			name: "invalid parameter for phone number transformer",
			config: &transformers.Config{
				Name:       transformers.String,
				Parameters: map[string]any{"invalid": "param"},
			},
			wantErr: transformers.ErrUnknownParameter,
		},
		{
			name: "unsupported transformer",
			config: &transformers.Config{
				Name:       "unsupported",
				Parameters: map[string]any{},
			},
			wantErr: transformers.ErrUnsupportedTransformer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tb := NewTransformerBuilder()
			_, err := tb.New(tt.config)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
