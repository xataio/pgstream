// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPhoneNumberTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		params     Parameters
		value      any
		wantPrefix string
		wantLen    int
		wantErr    error
	}{
		{
			name: "ok - string with prefix",
			params: Parameters{
				"prefix":     "(030) ",
				"min_length": 10,
				"max_length": 10,
			},
			value:      "12345",
			wantPrefix: "(030) ",
			wantLen:    10,
			wantErr:    nil,
		},
		{
			name: "ok - []byte without prefix",
			params: Parameters{
				"min_length": 6,
				"max_length": 6,
			},
			value:      []byte("12345"),
			wantPrefix: "",
			wantLen:    6,
			wantErr:    nil,
		},
		{
			name: "ok - []byte without prefix, deterministic generator",
			params: Parameters{
				"min_length": 6,
				"max_length": 6,
				"generator":  "deterministic",
			},
			value:      []byte("12345"),
			wantPrefix: "457059", // not prefix but the actual string, deterministic
			wantLen:    6,
			wantErr:    nil,
		},
		{
			name: "error - prefix longer than min_length",
			params: Parameters{
				"prefix":     "12345678",
				"min_length": 6,
				"max_length": 10,
			},
			value:   "12345",
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - max_length less than min_length",
			params: Parameters{
				"min_length": 10,
				"max_length": 8,
			},
			value:   "12345",
			wantErr: ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewPhoneNumberTransformer(tc.params)
			if tc.wantErr != nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			got, err := transformer.Transform(tc.value)
			require.NoError(t, err)

			gotStr, ok := got.(string)
			require.True(t, ok)
			require.Len(t, gotStr, tc.wantLen)
			if tc.wantPrefix != "" {
				require.True(t, len(gotStr) >= len(tc.wantPrefix))
				require.Equal(t, tc.wantPrefix, gotStr[:len(tc.wantPrefix)])
			}
		})
	}
}
