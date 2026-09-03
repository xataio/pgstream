// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

// the definition feeds the docs and transformers-definition.json while the
// method feeds rule validation; nothing else keeps the two in step
func TestTransformers_UniquenessMatchesDefinition(t *testing.T) {
	t.Parallel()

	cases := map[transformers.TransformerType]transformers.ParameterValues{
		transformers.Masking:                {"type": "default"},
		transformers.Email:                  {},
		transformers.Template:               {"template": "{{ .GetValue }}"},
		transformers.JSON:                   {"operations": []any{map[string]any{"operation": "set", "path": "a", "value": "b"}}},
		transformers.Hstore:                 {"operations": []any{map[string]any{"operation": "set", "key": "a", "value": "b"}}},
		transformers.PhoneNumber:            {"prefix": "+1", "min_length": 9, "max_length": 12},
		transformers.LiteralString:          {"literal": "fixed"},
		transformers.String:                 {},
		transformers.EncryptedAESSIV:        {"key_hex": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"},
		transformers.GreenmaskString:        {"min_length": 2, "max_length": 12},
		transformers.GreenmaskFirstName:     {},
		transformers.GreenmaskInteger:       {"min_value": 0, "max_value": 1000},
		transformers.GreenmaskFloat:         {"min_value": 0.0, "max_value": 1000.0},
		transformers.GreenmaskUUID:          {},
		transformers.GreenmaskBoolean:       {},
		transformers.GreenmaskChoice:        {"choices": []any{"alpha", "beta"}},
		transformers.GreenmaskUnixTimestamp: {"min_value": "946684800", "max_value": "1893456000"},
		transformers.GreenmaskDate:          {"min_value": "2020-01-01", "max_value": "2024-12-31"},
		transformers.GreenmaskUTCTimestamp:  {"min_timestamp": "2020-01-01T00:00:00Z", "max_timestamp": "2024-12-31T23:59:59Z"},
		transformers.NeosyncString:          {},
		transformers.NeosyncFirstName:       {},
		transformers.NeosyncLastName:        {},
		transformers.NeosyncFullName:        {},
		transformers.NeosyncEmail:           {},
		transformers.PGAnonymizer:           {"anon_function": "anon.fake_email()", "postgres_url": "postgres://user:pass@localhost:5432/db"},
		transformers.LookupChoice:           {"lookup_table": "public.countries", "lookup_column": "id", "postgres_url": "postgres://user:pass@localhost:5432/db"},
	}

	for transformerType := range TransformersMap {
		_, found := cases[transformerType]
		require.True(t, found, "transformer %q is missing a uniqueness test case", transformerType)
	}

	builder := NewTransformerBuilder()
	for transformerType, params := range cases {
		t.Run(string(transformerType), func(t *testing.T) {
			t.Parallel()

			transformer, err := builder.New(&transformers.Config{Name: transformerType, Parameters: params})
			require.NoError(t, err)
			require.Equal(t, TransformersMap[transformerType].Definition.Uniqueness, transformers.UniquenessOf(transformer))
		})
	}
}
