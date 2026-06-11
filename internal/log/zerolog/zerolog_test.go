// SPDX-License-Identifier: Apache-2.0

package zerolog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name: "empty config is valid",
			cfg:  Config{},
		},
		{
			name: "console with no_color",
			cfg:  Config{LogFormat: "console", NoColor: true},
		},
		{
			name: "json without no_color",
			cfg:  Config{LogFormat: "json"},
		},
		{
			name:    "json with no_color is rejected",
			cfg:     Config{LogFormat: "json", NoColor: true},
			wantErr: ErrNoColorUnderJSONFormat,
		},
		{
			name: "json with no_color=false is accepted",
			cfg:  Config{LogFormat: "json", NoColor: false},
		},
		{
			name:    "unsupported format",
			cfg:     Config{LogFormat: "xml"},
			wantErr: nil, // wrapped error, checked separately below
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.name == "unsupported format" {
				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid log format")
				return
			}
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
