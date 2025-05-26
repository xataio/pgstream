// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaskingTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  ParameterValues
		wantErr error
	}{
		{
			name:    "ok - valid default parameters",
			wantErr: nil,
		},
		{
			name: "ok - valid custom parameters",
			params: ParameterValues{
				"type": "password",
			},
			wantErr: nil,
		},
		{
			name: "error - invalid, custom masking",
			params: ParameterValues{
				"type":         "custom",
				"mask_begin":   "4",
				"unmask_begin": "4",
			},
			wantErr: errMaskUnmaskCannotBeUsedTogether,
		},
		{
			name: "error - invalid param type, custom masking",
			params: ParameterValues{
				"type":       "custom",
				"mask_begin": 4,
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - invalid masking type",
			params: ParameterValues{
				"type": "invalid",
			},
			wantErr: errInvalidMaskingType,
		},
		{
			name: "error - invalid parameter type",
			params: ParameterValues{
				"type": 123,
			},
			wantErr: ErrInvalidParameters,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mt, err := NewMaskingTransformer(tt.params)
			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantErr != nil {
				return
			}
			require.NoError(t, err)
			require.NotNil(t, mt)
		})
	}
}

func TestMaskingTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  ParameterValues
		input   any
		want    any
		wantErr error
	}{
		{
			name: "ok - password",
			params: ParameterValues{
				"type": "password",
			},
			input:   "aVeryStrongPassword123",
			want:    "************",
			wantErr: nil,
		},
		{
			name: "ok - name",
			params: ParameterValues{
				"type": "name",
			},
			input:   []byte("John Doe"),
			want:    "J**n D**e",
			wantErr: nil,
		},
		{
			name: "ok - address",
			params: ParameterValues{
				"type": "address",
			},
			input:   "123 Main St, Anytown, USA",
			want:    "123 Ma******",
			wantErr: nil,
		},
		{
			name: "ok - email",
			params: ParameterValues{
				"type": "email",
			},
			input:   "john.doe@example.com",
			want:    "joh****e@example.com",
			wantErr: nil,
		},
		{
			name: "ok - mobile",
			params: ParameterValues{
				"type": "mobile",
			},
			input:   "1234567890",
			want:    "1234***890",
			wantErr: nil,
		},
		{
			name: "ok - tel",
			params: ParameterValues{
				"type": "tel",
			},
			input:   "+1-23-456-789",
			want:    "(+1)2345-****",
			wantErr: nil,
		},
		{
			name: "ok - id",
			params: ParameterValues{
				"type": "id",
			},
			input:   "123456789",
			want:    "123456****",
			wantErr: nil,
		},
		{
			name: "ok - credit_card",
			params: ParameterValues{
				"type": "credit_card",
			},
			input:   "4111-1111-1111-1111",
			want:    "4111-1******11-1111",
			wantErr: nil,
		},
		{
			name: "ok - url",
			params: ParameterValues{
				"type": "url",
			},
			input:   "http://admin:mysecretpassword@localhost:1234/uri",
			want:    "http://admin:xxxxx@localhost:1234/uri",
			wantErr: nil,
		},
		{
			name: "ok - default",
			params: ParameterValues{
				"type": "default",
			},
			input:   "Sensitive Data",
			want:    "**************",
			wantErr: nil,
		},
		{
			name: "ok - custom masking",
			params: ParameterValues{
				"type": "custom",
			},
			input:   "Sensitive Data",
			want:    "**************",
			wantErr: nil,
		},
		{
			name: "ok - custom masking with mask indexes",
			params: ParameterValues{
				"type":       "custom",
				"mask_begin": "4",
				"mask_end":   "400%",
			},
			input:   "Sensitive Data",
			want:    "Sens**********",
			wantErr: nil,
		},
		{
			name: "ok - custom masking with unmask indexes",
			params: ParameterValues{
				"type":         "custom",
				"unmask_begin": "78%",
				"unmask_end":   "-4%",
			},
			input:   "Sensitive Data",
			want:    "Sensitive ****",
			wantErr: nil,
		},
		{
			name: "error - invalid input type",
			params: ParameterValues{
				"type": "default",
			},
			input:   123,
			want:    nil,
			wantErr: ErrUnsupportedValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mt, err := NewMaskingTransformer(tt.params)
			require.NoError(t, err)
			got, err := mt.Transform(context.Background(), Value{TransformValue: tt.input})
			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantErr != nil {
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}
