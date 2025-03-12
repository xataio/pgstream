// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewEmailTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.Parameters
		wantErr error
	}{
		{
			name:    "ok - valid default parameters",
			params:  transformers.Parameters{},
			wantErr: nil,
		},
		{
			name: "ok - valid custom parameters",
			params: transformers.Parameters{
				"email_type":           "fullname",
				"invalid_email_action": "generate",
				"excluded_domains":     []string{"example.com", "example.org"},
				"max_length":           10,
				"preserve_domain":      true,
				"preserve_length":      true,
				"seed":                 0,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid preserve_length",
			params: transformers.Parameters{
				"preserve_length": 1,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid preserve_domain",
			params: transformers.Parameters{
				"preserve_domain": 1,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid max_length",
			params: transformers.Parameters{
				"max_length": "1",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid seed",
			params: transformers.Parameters{
				"seed": "1",
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid excluded_domains, []any",
			params: transformers.Parameters{
				"excluded_domains": []any{"example.com", 3},
			},
			wantErr: errInvalidExcludedDomains,
		},
		{
			name: "error - invalid excluded_domains, int",
			params: transformers.Parameters{
				"excluded_domains": 3,
			},
			wantErr: errInvalidExcludedDomains,
		},
		{
			name: "error - invalid email_type",
			params: transformers.Parameters{
				"email_type": 1,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid email_type value",
			params: transformers.Parameters{
				"email_type": "invalid",
			},
			wantErr: errInvalidEmailType,
		},
		{
			name: "error - invalid invalid_email_action",
			params: transformers.Parameters{
				"invalid_email_action": 1,
			},
			wantErr: transformers.ErrInvalidParameters,
		},
		{
			name: "error - invalid invalid_email_action value",
			params: transformers.Parameters{
				"invalid_email_action": "invalid",
			},
			wantErr: errInvalidInvalidEmailAction,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewEmailTransformer(tt.params)
			require.ErrorIs(t, err, tt.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, transformer)
		})
	}
}

func TestEmailTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		input              string
		emailType          string
		invalidEmailAction string
		excludedDomains    any
		maxLength          int
		preserveDomain     bool
		preserveLength     bool
		seed               int

		wantEmail string
		wantErr   error
	}{
		{
			name:               "ok - valid custom parameters",
			input:              "myname@lastname.com",
			emailType:          "fullname",
			invalidEmailAction: "generate",
			excludedDomains:    []string{"example.com", "example.org"},
			maxLength:          0,
			preserveDomain:     false,
			preserveLength:     false,
			seed:               0,

			wantEmail: "machadopasqui@donga.com",
			wantErr:   nil,
		},
		{
			name:               "ok - valid custom parameters, preserve length",
			input:              "myname@lastname.com",
			emailType:          "fullname",
			invalidEmailAction: "generate",
			excludedDomains:    []string{"example.com", "example.org"},
			maxLength:          20,
			preserveDomain:     false,
			preserveLength:     true,
			seed:               0,

			wantEmail: "malisuaul@baike.com",
			wantErr:   nil,
		},
		{
			name:               "ok - valid custom parameters, preserve domain",
			input:              "myname@lastname.com",
			emailType:          "uuidv4",
			invalidEmailAction: "passthrough",
			excludedDomains:    "example.com, example.org",
			maxLength:          17,
			preserveDomain:     true,
			preserveLength:     false,
			seed:               0,

			wantEmail: "",
			wantErr:   nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			params := transformers.Parameters{
				"email_type":           tc.emailType,
				"invalid_email_action": tc.invalidEmailAction,
				"excluded_domains":     tc.excludedDomains,
				"max_length":           tc.maxLength,
				"preserve_domain":      tc.preserveDomain,
				"preserve_length":      tc.preserveLength,
				"seed":                 tc.seed,
			}
			transformer, err := NewEmailTransformer(params)
			require.NoError(t, err)
			got, err := transformer.Transform(tc.input)
			require.ErrorIs(t, err, tc.wantErr)
			require.NotNil(t, got)
			val, ok := got.(string)
			require.True(t, ok)
			require.NotEmpty(t, val)

			_, domainExpected, _ := strings.Cut(tc.input, "@")
			_, domainGot, found := strings.Cut(val, "@")
			require.True(t, found)
			if tc.preserveDomain {
				require.Equal(t, domainExpected, domainGot)
			}
			if tc.excludedDomains != nil {
				excludedDomains := []string{}
				switch v := tc.excludedDomains.(type) {
				case string:
					excludedDomains = strings.Split(v, ",")
				case []string:
					excludedDomains = v
				default:
					require.Fail(t, "unexpected type for excludedDomains")
				}
				require.NotContains(t, excludedDomains, domainGot)
			}

			if tc.preserveLength {
				require.Equal(t, len(tc.input), len(val))
			}

			if tc.maxLength != 0 {
				require.LessOrEqual(t, len(val), tc.maxLength)
			}

			if tc.wantEmail != "" {
				require.Equal(t, tc.wantEmail, val)
			}
		})
	}
}
