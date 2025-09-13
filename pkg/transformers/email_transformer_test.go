// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmailTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		params ParameterValues

		wantLen int
		wantEmail string
		wantErr error
	}{
		{
			name:  "ok - string",
			value: "hello@test.com",
			params: ParameterValues{
				"replacement_domain": "@example.com",
				"exclude_domain": "test.com",
			},
			wantLen: 14,
			wantEmail: "hello@test.com",
			wantErr: nil,
		},
		{
			name:  "ok - []byte",
			value: []byte("hello@test.com"),
			params: ParameterValues{
				"replacement_domain": "@example.com",
				"exclude_domain": "test.com",
				"salt": "customsalt",
			},

			wantLen: 14,
			wantEmail: "hello@test.com",
			wantErr: nil,
		},
		{
			name:  "ok - not excluded.com domain",
			value: "hello@notexcluded.com",
			params: ParameterValues{
				"replacement_domain": "@nondefault.com",
				"exclude_domain": "excluded.com",
			},

			wantLen: 35,
			wantEmail: "BYB5liPSx6sugfg2mewc@nondefault.com",
			wantErr: nil,
		},
		{
			name: "ok - array of emails",
			value: "{foo@bar.com,test@excluded.com}",
			params: ParameterValues{
				"replacement_domain": "@nondefaultcrypt.com",
				"exclude_domain": "excluded.com",
			},

			wantLen: 50,
			wantEmail: "{IllJlcIkRf@nondefaultcrypt.com,test@excluded.com}",
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

			st, err := NewEmailTransformer(tc.params)
			require.NoError(t, err)
			got, err := st.Transform(context.Background(), Value{TransformValue: tc.value})
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}

			if(tc.wantEmail != "") {
				require.Equal(t, tc.wantEmail, got.(string))
			}

			require.Len(t, got, tc.wantLen)
		})
	}
}
