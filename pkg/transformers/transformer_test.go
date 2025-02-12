// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_FindParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		params    Parameters
		paramName string

		wantFound bool
		wantParam int
		wantErr   error
	}{
		{
			name: "ok",
			params: map[string]any{
				"test": 1,
			},
			paramName: "test",

			wantFound: true,
			wantParam: 1,
			wantErr:   nil,
		},
		{
			name: "ok - not found",
			params: map[string]any{
				"test": 1,
			},
			paramName: "another",

			wantFound: false,
			wantParam: 0,
			wantErr:   nil,
		},
		{
			name: "error - invalid parameter type",
			params: map[string]any{
				"test": "1",
			},
			paramName: "test",

			wantFound: true,
			wantParam: 0,
			wantErr:   ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, found, err := FindParameter[int](tc.params, tc.paramName)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantFound, found)
			require.Equal(t, tc.wantParam, got)
		})
	}
}
