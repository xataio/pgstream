// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"testing"

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
				"ratio":             0.1,
				"sigma":             2.5,
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
			name: "validation error",
			params: ParameterValues{
				"anon_function": "invalid_function",
				"postgres_url":  "postgres://user:pass@localhost/db",
			},
			expectedErr: "pg_anonymizer_transformer: anon_function must start with 'anon.'",
		},
		{
			name: "invalid interval format",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"interval":      "invalid_interval",
			},
			expectedErr: "pg_anonymizer_transformer: interval must be a valid PostgreSQL interval",
		},
		{
			name: "non-string anon_function parameter",
			params: ParameterValues{
				"anon_function": 123,
				"postgres_url":  "postgres://user:pass@localhost/db",
			},
			expectedErr: "pg_anonymizer_transformer: anon_function must be a string",
		},
		{
			name: "non-string postgres_url parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  123,
			},
			expectedErr: "pg_anonymizer_transformer: postgres_url must be a string",
		},
		{
			name: "non-string salt parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"salt":          123,
			},
			expectedErr: "pg_anonymizer_transformer: salt must be a string",
		},
		{
			name: "non-string hash_algorithm parameter",
			params: ParameterValues{
				"anon_function":  "anon.pseudo_email",
				"postgres_url":   "postgres://user:pass@localhost/db",
				"hash_algorithm": 123,
			},
			expectedErr: "pg_anonymizer_transformer: hash_algorithm must be a string",
		},
		{
			name: "non-string interval parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"interval":      123,
			},
			expectedErr: "pg_anonymizer_transformer: interval must be a string",
		},
		{
			name: "non-float ratio parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"ratio":         "123.45",
			},
			expectedErr: "pg_anonymizer_transformer: ratio must be a float",
		},
		{
			name: "non-float sigma parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"sigma":         "123.45",
			},
			expectedErr: "pg_anonymizer_transformer: sigma must be a float",
		},
		{
			name: "non-string mask parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"mask":          123,
			},
			expectedErr: "pg_anonymizer_transformer: mask must be a string",
		},
		{
			name: "non-integer mask_prefix_count parameter",
			params: ParameterValues{
				"anon_function":     "anon.pseudo_email",
				"postgres_url":      "postgres://user:pass@localhost/db",
				"mask_prefix_count": "not_an_integer",
			},
			expectedErr: "pg_anonymizer_transformer: mask_prefix_count must be an integer",
		},
		{
			name: "non-integer mask_suffix_count parameter",
			params: ParameterValues{
				"anon_function":     "anon.pseudo_email",
				"postgres_url":      "postgres://user:pass@localhost/db",
				"mask_suffix_count": "not_an_integer",
			},
			expectedErr: "pg_anonymizer_transformer: mask_suffix_count must be an integer",
		},
		{
			name: "non-string min parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"min":           123,
			},
			expectedErr: "pg_anonymizer_transformer: min must be a string",
		},
		{
			name: "non-string max parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"max":           123,
			},
			expectedErr: "pg_anonymizer_transformer: max must be a string",
		},
		{
			name: "non-string range parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"range":         123,
			},
			expectedErr: "pg_anonymizer_transformer: range must be a string",
		},
		{
			name: "non-string locale parameter",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"locale":        123,
			},
			expectedErr: "pg_anonymizer_transformer: locale must be a string",
		},
		{
			name: "only min provided",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"min":           "1",
			},
			expectedErr: "pg_anonymizer_transformer: both min and max must be provided together",
		},
		{
			name: "only max provided",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"max":           "100",
			},
			expectedErr: "pg_anonymizer_transformer: both min and max must be provided together",
		},
		{
			name: "both range bounds provided",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"min":           "1",
				"max":           "100",
			},
		},
		{
			name: "range parameter provided",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"range":         "[1,100)",
			},
		},
		{
			name: "locale parameter provided",
			params: ParameterValues{
				"anon_function": "anon.pseudo_email",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"locale":        "fr_FR",
			},
		},
		{
			name: "non-int count parameter",
			params: ParameterValues{
				"anon_function": "anon.lorem_ipsum",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"count":         "123",
			},
			expectedErr: "pg_anonymizer_transformer: count must be an integer",
		},
		{
			name: "non-string unit parameter",
			params: ParameterValues{
				"anon_function": "anon.lorem_ipsum",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"unit":          123,
			},
			expectedErr: "pg_anonymizer_transformer: unit must be a string",
		},
		{
			name: "invalid unit parameter",
			params: ParameterValues{
				"anon_function": "anon.lorem_ipsum",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"unit":          "invalid_unit",
			},
			expectedErr: "pg_anonymizer_transformer: unit must be one of 'characters', 'words', or 'paragraphs'",
		},
		{
			name: "valid unit parameter - characters",
			params: ParameterValues{
				"anon_function": "anon.lorem_ipsum",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"unit":          "characters",
				"count":         10,
			},
		},
		{
			name: "count parameter provided",
			params: ParameterValues{
				"anon_function": "anon.random_string",
				"postgres_url":  "postgres://user:pass@localhost/db",
				"count":         10,
			},
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
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					require.Equal(t, "SELECT anon.random_hash($1)", query)
					require.Equal(t, []any{"value"}, args)
					require.Len(t, dest, 1)
					ptr, ok := dest[0].(*any)
					require.True(t, ok)
					*ptr = testHash
					return nil
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
				QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return errTest
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
			defer require.NoError(t, transformer.Close())

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
				ratio:  0.1,
			},
			value:     100,
			valueType: "integer",
			wantQuery: "SELECT anon.noise($1::integer, $2)",
			wantArgs:  []any{100, 0.1},
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
				sigma:  2.5,
			},
			value:     "image_data",
			valueType: "bytea",
			wantQuery: "SELECT anon.image_blur($1, $2)",
			wantArgs:  []any{"image_data", 2.5},
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
				anonFn: "anon.random_date()",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.random_date()",
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
		{
			name: "random_date_between function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_date_between",
				min:    "2023-01-01",
				max:    "2023-12-31",
			},
			value:     "any_value",
			valueType: "date",
			wantQuery: "SELECT anon.random_date_between($1, $2)",
			wantArgs:  []any{"2023-01-01", "2023-12-31"},
		},
		{
			name: "random_int_between function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_int_between",
				min:    "1",
				max:    "100",
			},
			value:     "any_value",
			valueType: "integer",
			wantQuery: "SELECT anon.random_int_between($1, $2)",
			wantArgs:  []any{"1", "100"},
		},
		{
			name: "random_bigint_between function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_bigint_between",
				min:    "1000000",
				max:    "9999999",
			},
			value:     "any_value",
			valueType: "bigint",
			wantQuery: "SELECT anon.random_bigint_between($1, $2)",
			wantArgs:  []any{"1000000", "9999999"},
		},
		{
			name: "random_in_int4range function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in_int4range",
				rangeStr: "[1,100)",
			},
			value:     "any_value",
			valueType: "int4range",
			wantQuery: "SELECT anon.random_in_int4range($1)",
			wantArgs:  []any{"[1,100)"},
		},
		{
			name: "random_in_int8range function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in_int8range",
				rangeStr: "[1000000,9999999)",
			},
			value:     "any_value",
			valueType: "int8range",
			wantQuery: "SELECT anon.random_in_int8range($1)",
			wantArgs:  []any{"[1000000,9999999)"},
		},
		{
			name: "random_in_daterange function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in_daterange",
				rangeStr: "[2023-01-01,2023-12-31)",
			},
			value:     "any_value",
			valueType: "daterange",
			wantQuery: "SELECT anon.random_in_daterange($1)",
			wantArgs:  []any{"[2023-01-01,2023-12-31)"},
		},
		{
			name: "random_in_numrange function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in_numrange",
				rangeStr: "[0.0,1.0)",
			},
			value:     "any_value",
			valueType: "numrange",
			wantQuery: "SELECT anon.random_in_numrange($1)",
			wantArgs:  []any{"[0.0,1.0)"},
		},
		{
			name: "random_in_tsrange function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in_tsrange",
				rangeStr: "[2023-01-01 00:00:00,2023-12-31 23:59:59)",
			},
			value:     "any_value",
			valueType: "tsrange",
			wantQuery: "SELECT anon.random_in_tsrange($1)",
			wantArgs:  []any{"[2023-01-01 00:00:00,2023-12-31 23:59:59)"},
		},
		{
			name: "random_in_tstzrange function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in_tstzrange",
				rangeStr: "[2023-01-01 00:00:00+00,2023-12-31 23:59:59+00)",
			},
			value:     "any_value",
			valueType: "tstzrange",
			wantQuery: "SELECT anon.random_in_tstzrange($1)",
			wantArgs:  []any{"[2023-01-01 00:00:00+00,2023-12-31 23:59:59+00)"},
		},
		{
			name: "random_in_enum function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in_enum",
				rangeStr: "NULL::COLOR",
			},
			value:     "any_value",
			valueType: "color_enum",
			wantQuery: "SELECT anon.random_in_enum($1)",
			wantArgs:  []any{"NULL::COLOR"},
		},
		{
			name: "random_in function",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in",
				rangeStr: "ARRAY['red', 'green', 'blue']",
			},
			value:     "any_value",
			valueType: "color_enum",
			wantQuery: "SELECT anon.random_in(ARRAY['red', 'green', 'blue'])",
			wantArgs:  nil,
		},
		{
			name: "dummy function with locale",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.dummy_first_name_locale",
				locale: "fr_FR",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.dummy_first_name_locale($1)",
			wantArgs:  []any{"fr_FR"},
		},
		{
			name: "lorem_ipsum function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.lorem_ipsum",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.lorem_ipsum()",
			wantArgs:  []any{},
		},
		{
			name: "dummy function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.dummy_last_name",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.dummy_last_name()",
			wantArgs:  []any{},
		},
		{
			name: "fake function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.fake_company",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.fake_company()",
			wantArgs:  []any{},
		},
		{
			name: "lorem_ipsum function with count",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.lorem_ipsum",
				count:  5,
				unit:   "word",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.lorem_ipsum(word := $1)",
			wantArgs:  []any{5},
		},
		{
			name: "lorem_ipsum function with count and character unit",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.lorem_ipsum",
				count:  100,
				unit:   "character",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.lorem_ipsum(character := $1)",
			wantArgs:  []any{100},
		},
		{
			name: "lorem_ipsum function with count and paragraph unit",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.lorem_ipsum",
				count:  3,
				unit:   "paragraph",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.lorem_ipsum(paragraph := $1)",
			wantArgs:  []any{3},
		},
		{
			name: "random_string function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_string",
				count:  10,
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.random_string($1)",
			wantArgs:  []any{10},
		},
		{
			name: "random_phone function",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_phone",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.random_phone()",
			wantArgs:  nil,
		},
		{
			name: "random_phone function with prefix",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_phone",
				prefix: "+15",
			},
			value:     "any_value",
			valueType: "text",
			wantQuery: "SELECT anon.random_phone($1)",
			wantArgs:  []any{"+15"},
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
				ratio:  0,
			},
			wantErr: errNoiseRatioRequired,
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
				sigma:  0,
			},
			wantErr: errImageBlurSigmaRequired,
		},
		{
			name: "function contains semicolon - invalid",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.pseudo_email; DROP TABLE users;",
			},
			wantErr: errAnonFunctionInvalid,
		},
		{
			name: "random_date_between function - missing lower bound",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_date_between",
				min:    "",
				max:    "2023-12-31",
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "random_date_between function - missing upper bound",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_date_between",
				min:    "2023-01-01",
				max:    "",
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "random_int_between function - valid bounds",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_int_between",
				min:    "1",
				max:    "100",
			},
			wantErr: nil,
		},
		{
			name: "random_in_enum function - missing range",
			transformer: &PGAnonymizerTransformer{
				anonFn:   "anon.random_in_enum",
				rangeStr: "",
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "function not allowed",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.unknown_function",
			},
			wantErr: errAnonFunctionNotAllowed,
		},
		{
			name: "random_string function - missing count",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_string",
				count:  0,
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "random_string function - valid count",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_string",
				count:  10,
			},
			wantErr: nil,
		},
		{
			name: "random_phone function - missing count",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_phone",
				count:  0,
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "random_phone function - valid count",
			transformer: &PGAnonymizerTransformer{
				anonFn: "anon.random_phone",
				count:  15,
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
