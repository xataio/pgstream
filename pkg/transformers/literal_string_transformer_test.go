// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiteralStringTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  Parameters
		wantErr error
	}{
		{
			name: "ok - valid",
			params: Parameters{
				"literal": "test",
			},
			wantErr: nil,
		},
		{
			name: "error - invalid literal",
			params: Parameters{
				"literal": 123,
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name:    "error - empty literal",
			params:  Parameters{},
			wantErr: errLiteralStringCannotBeEmpty,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lst, err := NewLiteralStringTransformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}
			require.NoError(t, err)
			require.NotNil(t, lst)
		})
	}
}

func TestLiteralStringTransformer_Transform(t *testing.T) {
	t.Parallel()
	wantOutput := "{'output': 'testoutput'"
	lst, err := NewLiteralStringTransformer(Parameters{"literal": wantOutput})
	require.NoError(t, err)
	tests := []struct {
		name    string
		params  Parameters
		input   any
		want    any
		wantErr error
	}{
		{
			name:    "ok - string",
			input:   "testinput",
			wantErr: nil,
		},
		{
			name:    "ok - JSON",
			input:   "{'json': 'jsoninput'}",
			wantErr: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := lst.Transform(Value{TransformValue: tc.input})
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}
			require.Equal(t, wantOutput, got)
		})
	}
}
