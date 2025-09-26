// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"testing"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibmocks "github.com/xataio/pgstream/internal/postgres/mocks"

	"github.com/stretchr/testify/require"
)

func TestNewPGAnonymizerTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		params      ParameterValues
		expectedErr string
	}{
		{
			name: "successful creation with minimal params",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
			},
		},
		{
			name: "successful creation with all params",
			params: ParameterValues{
				"anon_function":     "anon.partial",
				"postgres_url":      "postgres://user:pass@localhost/db",
				"salt":              "mysalt",
				"hash_algorithm":    "sha256",
				"interval":          "1 day",
				"ratio":             "0.1",
				"sigma":             "2.5",
				"mask":              "***",
				"mask_prefix_count": 2,
				"mask_suffix_count": 3,
			},
		},
		{
			name: "missing anon_function",
			params: ParameterValues{
				"postgres_url": "postgres://user:pass@localhost/db",
			},
			expectedErr: "pg_anonymizer_transformer: anon_function parameter not found",
		},
		{
			name: "missing postgres_url",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
			},
			expectedErr: "pg_anonymizer_transformer: postgres_url parameter not found",
		},
		{
			name: "invalid hash_algorithm",
			params: ParameterValues{
				"anon_function":  "anon.pseudo_email",
				"postgres_url":   "postgres://user:pass@localhost/db",
				"hash_algorithm": "invalid_algorithm",
			},
			expectedErr: "pg_anonymizer_transformer: unsupported hash_algorithm: invalid_algorithm",
		},
		{
			name: "invalid anon_function - does not start with anon.",
			params: ParameterValues{
				"anon_function": "invalid_function",
				"postgres_url":  "postgres://user:pass@localhost/db",
			},
			expectedErr: "pg_anonymizer_transformer: anon_function must start with 'anon.'",
		},
		{
			name: "digest function without salt",
			params: ParameterValues{
				"anon_function": "anon.digest",
				"postgres_url":  "postgres://user:pass@localhost/db",
			},
			expectedErr: "pg_anonymizer_transformer: salt is required for anon.digest function",
		},
		{
			name: "noise function without ratio",
			params: ParameterValues{
				"anon_function": "anon.noise",
				"postgres_url":  "postgres://user:pass@localhost/db",
			},
			expectedErr: "pg_anonymizer_transformer: ratio is required for anon.noise function",
		},
		{
			name: "partial function with negative prefix count",
			params: ParameterValues{
				"anon_function":     "anon.partial",
				"postgres_url":      "postgres://user:pass@localhost/db",
				"mask":              "***",
				"mask_prefix_count": -1,
			},
			expectedErr: "pg_anonymizer_transformer: mask_prefix_count must be non-negative for anon.partial function",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewPGAnonymizerTransformer(tc.params)

			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
				require.Nil(t, transformer)
			} else {
				require.NoError(t, err)
				require.NotNil(t, transformer)
				require.Equal(t, PGAnonymizer, transformer.Type())
				require.Equal(t, pgAnonymizerCompatibleTypes, transformer.CompatibleTypes())
			}
		})
	}
}

func TestPGAnonymizerTransformer_Transform(t *testing.T) {
	t.Parallel()

	testHash := "59eace21dad6aa8e649b650efc8c81b34c08ec5eab7062113e84fc3f7185ebf6"
	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		anonFn string
		conn   *pglibmocks.Querier
		value  Value

		wantResult any
		wantErr    error
	}{
		{
			name:   "successful transformation",
			anonFn: "anon.random_hash",
			conn: &pglibmocks.Querier{
				QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
					require.Equal(t, "SELECT anon.random_hash('value')", query)
					return &pglibmocks.Row{
						ScanFn: func(dest ...any) error {
							require.Len(t, dest, 1)
							if ptr, ok := dest[0].(*any); ok {
								*ptr = testHash
							}
							return nil
						},
					}
				},
			},
			value: Value{
				TransformValue: "value",
				TransformType:  "text",
			},
			wantResult: testHash,
			wantErr:    nil,
		},
		{
			name:   "error executing anon function",
			anonFn: "anon.random_hash",
			conn: &pglibmocks.Querier{
				QueryRowFn: func(ctx context.Context, query string, args ...any) pglib.Row {
					require.Equal(t, "SELECT anon.random_hash('value')", query)
					return &pglibmocks.Row{
						ScanFn: func(dest ...any) error {
							return errTest
						},
					}
				},
			},
			value: Value{
				TransformValue: "value",
				TransformType:  "text",
			},
			wantResult: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer := &PGAnonymizerTransformer{
				anonFn: tc.anonFn,
				conn:   tc.conn,
			}

			got, err := transformer.Transform(context.Background(), tc.value)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantResult, got)
		})
	}
}

func TestPGAnonymizerTransformer_getAnonFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		transformer *PGAnonymizerTransformer
		value       any
		valueType   string

		wantFn string
	}{
		{
			name: "pseudo function with salt",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.pseudo_email",
				salt:   "mysalt",
			},
			value:     "test@example.com",
			valueType: "text",
			wantFn:    "anon.pseudo_email('test@example.com'::text, 'mysalt')",
		},
		{
			name: "pseudo function without salt",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.pseudo_phone",
				salt:   "",
			},
			value:     "555-1234",
			valueType: "varchar",
			wantFn:    "anon.pseudo_phone('555-1234'::varchar)",
		},
		{
			name: "random_hash function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_hash",
			},
			value:     "sensitive_data",
			valueType: "text",
			wantFn:    "anon.random_hash('sensitive_data')",
		},
		{
			name: "hash function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.hash",
			},
			value:     "password123",
			valueType: "text",
			wantFn:    "anon.hash('password123')",
		},
		{
			name: "partial_email function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.partial_email",
			},
			value:     "user@domain.com",
			valueType: "text",
			wantFn:    "anon.partial_email('user@domain.com')",
		},
		{
			name: "digest function",
			transformer: &PGAnonymizerTransformer{
				anonFn:        "anon.digest",
				salt:          "salt123",
				hashAlgorithm: "sha256",
			},
			value:     "data",
			valueType: "text",
			wantFn:    "anon.digest('data', 'salt123', 'sha256')",
		},
		{
			name: "noise function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.noise",
				ratio:  "0.1",
			},
			value:     100,
			valueType: "integer",
			wantFn:    "anon.noise(100, 0.1)",
		},
		{
			name: "dnoise function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.dnoise",
				interval: "1 day",
			},
			value:     "2023-01-01",
			valueType: "date",
			wantFn:    "anon.dnoise('2023-01-01'::date, '1 day')",
		},
		{
			name: "image_blur function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.image_blur",
				sigma:  "2.5",
			},
			value:     "image_data",
			valueType: "bytea",
			wantFn:    "anon.image_blur('image_data', 2.5)",
		},
		{
			name: "partial function",
			transformer: &PGAnonymizerTransformer{
				anonFn:          "anon.partial",
				mask:            "***",
				maskPrefixCount: 2,
				maskSuffixCount: 3,
			},
			value:     "sensitive",
			valueType: "text",
			wantFn:    "anon.partial('sensitive', 2, '***', 3)",
		},
		{
			name: "constant function without parameters",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_string(10)",
			},
			value:     "any_value",
			valueType: "text",
			wantFn:    "anon.random_string(10)",
		},
		{
			name: "constant function without parenthesis or parameters",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.fake_first_name",
			},
			value:     "any_value",
			valueType: "text",
			wantFn:    "anon.fake_first_name()",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotFn := tc.transformer.getAnonFunction(tc.value, tc.valueType)
			require.Equal(t, tc.wantFn, gotFn)
		})
	}
}

func TestPGAnonymizerTransformer_validateAnonFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		transformer *PGAnonymizerTransformer
		wantErr     error
	}{
		{
			name: "valid pseudo function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.pseudo_email",
			},
			wantErr: nil,
		},
		{
			name: "invalid function - does not start with anon.",
			transformer: &PGAnonymizerTransformer{
				anonFn: "invalid_function",
			},
			wantErr: errAnonFunctionInvalid,
		},
		{
			name: "invalid function - too short",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon",
			},
			wantErr: errAnonFunctionInvalid,
		},
		{
			name: "digest function - missing hash algorithm",
			transformer: &PGAnonymizerTransformer{
				anonFn:        "anon.digest",
				salt:          "salt123",
				hashAlgorithm: "",
			},
			wantErr: errDigestHashAlgorithmRequired,
		},
		{
			name: "digest function - missing salt",
			transformer: &PGAnonymizerTransformer{
				anonFn:        "anon.digest",
				salt:          "",
				hashAlgorithm: "sha256",
			},
			wantErr: errDigestSaltRequired,
		},
		{
			name: "digest function - valid",
			transformer: &PGAnonymizerTransformer{
				anonFn:        "anon.digest",
				salt:          "salt123",
				hashAlgorithm: "sha256",
			},
			wantErr: nil,
		},
		{
			name: "noise function - missing ratio",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.noise",
				ratio:  "",
			},
			wantErr: errNoiseRatioRequired,
		},
		{
			name: "noise function - valid",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.noise",
				ratio:  "0.1",
			},
			wantErr: nil,
		},
		{
			name: "dnoise function - missing interval",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.dnoise",
				interval: "",
			},
			wantErr: errDnoiseIntervalRequired,
		},
		{
			name: "dnoise function - valid",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.dnoise",
				interval: "1 day",
			},
			wantErr: nil,
		},
		{
			name: "partial function - missing mask",
			transformer: &PGAnonymizerTransformer{
				anonFn:          "anon.partial",
				mask:            "",
				maskPrefixCount: 2,
				maskSuffixCount: 3,
			},
			wantErr: errPartialMaskRequired,
		},
		{
			name: "partial function - negative prefix count",
			transformer: &PGAnonymizerTransformer{
				anonFn:          "anon.partial",
				mask:            "***",
				maskPrefixCount: -1,
				maskSuffixCount: 3,
			},
			wantErr: errPartialPrefixInvalid,
		},
		{
			name: "partial function - negative suffix count",
			transformer: &PGAnonymizerTransformer{
				anonFn:          "anon.partial",
				mask:            "***",
				maskPrefixCount: 2,
				maskSuffixCount: -1,
			},
			wantErr: errPartialSuffixInvalid,
		},
		{
			name: "partial function - valid",
			transformer: &PGAnonymizerTransformer{
				anonFn:          "anon.partial",
				mask:            "***",
				maskPrefixCount: 2,
				maskSuffixCount: 3,
			},
			wantErr: nil,
		},
		{
			name: "image_blur function - missing sigma",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.image_blur",
				sigma:  "",
			},
			wantErr: errImageBlurSigmaRequired,
		},
		{
			name: "image_blur function - valid",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.image_blur",
				sigma:  "2.5",
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.transformer.validateAnonFunction()
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
