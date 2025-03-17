// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any

		wantLen int
		wantErr error
	}{
		{
			name:  "ok - string",
			value: "hello",

			wantLen: 5,
			wantErr: nil,
		},
		{
			name:  "ok - []byte",
			value: []byte("hello"),

			wantLen: 5,
			wantErr: nil,
		},
		{
			name:  "unsupported type",
			value: 1,

			wantLen: 0,
			wantErr: ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st, err := NewStringTransformer(nil)
			require.NoError(t, err)
			got, err := st.Transform(tc.value)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}

			require.Len(t, got, tc.wantLen)
			require.NotEqual(t, got, tc.value)
		})
	}
}
