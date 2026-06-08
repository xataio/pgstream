// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpandBracedEnvVars(t *testing.T) {
	t.Setenv("PGSTREAM_CONFIG_EXPANSION_VALUE", "expanded")
	t.Setenv("name", "should-not-expand")
	unsetEnv(t, "PGSTREAM_CONFIG_EXPANSION_MISSING")

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "expands braced variables",
			input: "url: ${PGSTREAM_CONFIG_EXPANSION_VALUE}",
			want:  "url: expanded",
		},
		{
			name:  "leaves bare variables untouched",
			input: "url: $PGSTREAM_CONFIG_EXPANSION_VALUE",
			want:  "url: $PGSTREAM_CONFIG_EXPANSION_VALUE",
		},
		{
			name:  "leaves regex and template dollar syntax untouched",
			input: `replacement: "$1-${1}-$name-$$"`,
			want:  `replacement: "$1-${1}-$name-$$"`,
		},
		{
			name:  "undefined braced variables expand to empty string",
			input: "before ${PGSTREAM_CONFIG_EXPANSION_MISSING} after",
			want:  "before  after",
		},
		{
			name:  "leaves malformed braced syntax untouched",
			input: "${} ${ PGSTREAM_CONFIG_EXPANSION_VALUE} ${PGSTREAM_CONFIG_EXPANSION_VALUE",
			want:  "${} ${ PGSTREAM_CONFIG_EXPANSION_VALUE} ${PGSTREAM_CONFIG_EXPANSION_VALUE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, expandBracedEnvVars(tt.input))
		})
	}
}

func TestParseTransformerConfig_ExpandsOnlyBracedEnvVars(t *testing.T) {
	t.Setenv("PGSTREAM_CONFIG_TEMPLATE_VALUE", "expanded")
	t.Setenv("PGSTREAM_CONFIG_BARE_VALUE", "should-not-expand")

	filename := filepath.Join(t.TempDir(), "transformers.yaml")
	require.NoError(t, os.WriteFile(filename, []byte(`
transformations:
  validation_mode: relaxed
  table_transformers:
    - schema: public
      table: test
      column_transformers:
        name:
          name: template
          parameters:
            template: "${PGSTREAM_CONFIG_TEMPLATE_VALUE} $PGSTREAM_CONFIG_BARE_VALUE $1 ${1} $$"
            replacement: "$1-${1}-$PGSTREAM_CONFIG_BARE_VALUE"
`), 0o600))

	cfg, err := ParseTransformerConfig(filename)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Len(t, cfg.TransformerRules, 1)

	columnRules := cfg.TransformerRules[0].ColumnRules
	require.Contains(t, columnRules, "name")
	require.Equal(t, "expanded $PGSTREAM_CONFIG_BARE_VALUE $1 ${1} $$", columnRules["name"].Parameters["template"])
	require.Equal(t, "$1-${1}-$PGSTREAM_CONFIG_BARE_VALUE", columnRules["name"].Parameters["replacement"])
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()

	previousValue, wasSet := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		if wasSet {
			require.NoError(t, os.Setenv(key, previousValue))
			return
		}
		require.NoError(t, os.Unsetenv(key))
	})
}
