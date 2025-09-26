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
					require.Equal(t, "SELECT anon.random_hash($1)", query)
					require.Equal(t, []any{"value"}, args)
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

func TestPGAnonymizerTransformer_buildParameterizedQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		transformer *PGAnonymizerTransformer
		value       any
		valueType   string

		wantQuery string
		wantArgs  []any
	}{
		{
			name: "pseudo function with salt",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.pseudo_email",
				salt:   "mysalt",
			},
			value:     "test@example.com",
			valueType: "text",
			wantQuery: "SELECT anon.pseudo_email($1::text, $2)",
			wantArgs:  []any{"test@example.com", "mysalt"},
		},
		{
			name: "pseudo function without salt",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.pseudo_phone",
				salt:   "",
			},
			value:     "555-1234",
			valueType: "varchar",
			wantQuery: "SELECT anon.pseudo_phone($1::varchar)",
			wantArgs:  []any{"555-1234"},
		},
		{
			name: "random_hash function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_hash",
			},
			value:     "sensitive_data",
			valueType: "text",
			wantQuery: "SELECT anon.random_hash($1)",
			wantArgs:  []any{"sensitive_data"},
		},
		{
			name: "hash function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.hash",
			},
			value:     "password123",
			valueType: "text",
			wantQuery: "SELECT anon.hash($1)",
			wantArgs:  []any{"password123"},
		},
		{
			name: "partial_email function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.partial_email",
			},
			value:     "user@domain.com",
			valueType: "text",
			wantQuery: "SELECT anon.partial_email($1)",
			wantArgs:  []any{"user@domain.com"},
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
			wantQuery: "SELECT anon.digest($1, $2, $3)",
			wantArgs:  []any{"data", "salt123", "sha256"},
		},
		{
			name: "noise function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.noise",
				ratio:  "0.1",
			},
			value:     100,
			valueType: "integer",
			wantQuery: "SELECT anon.noise($1, $2::numeric)",
			wantArgs:  []any{100, "0.1"},
		},
		{
			name: "dnoise function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.dnoise",
				interval: "1 day",
			},
			value:     "2023-01-01",
			valueType: "date",
			wantQuery: "SELECT anon.dnoise($1::date, $2::interval)",
			wantArgs:  []any{"2023-01-01", "1 day"},
		},
		{
			name: "image_blur function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.image_blur",
				sigma:  "2.5",
			},
			value:     "image_data",
			valueType: "bytea",
			wantQuery: "SELECT anon.image_blur($1, $2::numeric)",
			wantArgs:  []any{"image_data", "2.5"},
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
			wantQuery: "SELECT anon.partial($1, $2, $3, $4)",
			wantArgs:  []any{"sensitive", 2, "***", 3},
		},
		{
			name: "constant function without parameters",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_string(10)",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.random_string(10)",
			wantArgs:  []any{},
		},
		{
			name: "constant function without parenthesis or parameters",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.fake_first_name",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.fake_first_name()",
			wantArgs:  []any{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotQuery, gotArgs := tc.transformer.buildParameterizedQuery(tc.value, tc.valueType)
			require.Equal(t, tc.wantQuery, gotQuery)
			require.Equal(t, tc.wantArgs, gotArgs)
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
