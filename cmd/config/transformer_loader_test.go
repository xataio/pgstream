// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeTransformerLoaderTestFile(t *testing.T, dir, name, contents string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestTransformerConfigLoader_YAMLInlineRules(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configFile := writeTransformerLoaderTestFile(t, dir, "config.yaml", `
modifiers:
  transformations:
    validation_mode: relaxed
    table_transformers:
      - schema: Public
        table: Users
        column_transformers:
          Email:
            name: string
`)

	load := NewTransformerConfigLoader(configFile, "")
	require.NotNil(t, load)

	cfg, err := load(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "relaxed", cfg.ValidationMode)
	require.Len(t, cfg.TransformerRules, 1)
	require.Equal(t, "Public", cfg.TransformerRules[0].Schema)
	require.Equal(t, "Users", cfg.TransformerRules[0].Table)
	require.Contains(t, cfg.TransformerRules[0].ColumnRules, "Email")
	require.Equal(t, "string", cfg.TransformerRules[0].ColumnRules["Email"].Name)

	// The loader must re-read the file on every call rather than caching the
	// initial configuration.
	writeTransformerLoaderTestFile(t, dir, "config.yaml", `
modifiers:
  transformations:
    validation_mode: strict
    table_transformers:
      - schema: public
        table: users
        column_transformers:
          Phone:
            name: string
`)
	cfg, err = load(context.Background())
	require.NoError(t, err)
	require.Equal(t, "strict", cfg.ValidationMode)
	require.Len(t, cfg.TransformerRules, 1)
	require.Contains(t, cfg.TransformerRules[0].ColumnRules, "Phone")
	require.NotContains(t, cfg.TransformerRules[0].ColumnRules, "Email")
}

func TestTransformerConfigLoader_TransformerRulesFile(t *testing.T) {
	t.Setenv("PGSTREAM_RELOAD_TEST_VALUE", "expanded")

	dir := t.TempDir()
	rulesFile := writeTransformerLoaderTestFile(t, dir, "transformers.yaml", `
transformations:
  validation_mode: strict
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: template
          parameters:
            template: "${PGSTREAM_RELOAD_TEST_VALUE}"
`)

	load := NewTransformerConfigLoader("", rulesFile)
	require.NotNil(t, load)

	cfg, err := load(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "strict", cfg.ValidationMode)
	require.Len(t, cfg.TransformerRules, 1)
	require.Equal(t, "expanded", cfg.TransformerRules[0].ColumnRules["email"].Parameters["template"])

	writeTransformerLoaderTestFile(t, dir, "transformers.yaml", `
transformations:
  validation_mode: relaxed
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        phone:
          name: string
`)
	cfg, err = load(context.Background())
	require.NoError(t, err)
	require.Equal(t, "relaxed", cfg.ValidationMode)
	require.Contains(t, cfg.TransformerRules[0].ColumnRules, "phone")
	require.NotContains(t, cfg.TransformerRules[0].ColumnRules, "email")
}

func TestTransformerConfigLoader_NoSources(t *testing.T) {
	t.Parallel()

	require.Nil(t, NewTransformerConfigLoader("", ""))
}

func TestTransformerConfigLoader_InvalidSources(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	tests := []struct {
		name        string
		configFile  string
		rulesFile   string
		writeConfig string
		writeRules  string
	}{
		{
			name:        "missing yaml config",
			configFile:  filepath.Join(dir, "missing.yaml"),
			writeConfig: "",
		},
		{
			name:        "malformed yaml config",
			configFile:  filepath.Join(dir, "malformed.yaml"),
			writeConfig: "modifiers: [",
		},
		{
			name:        "yaml config without transformations",
			configFile:  filepath.Join(dir, "empty.yaml"),
			writeConfig: "modifiers: {}",
		},
		{
			name:        "missing rules file",
			rulesFile:   filepath.Join(dir, "missing-rules.yaml"),
			writeConfig: "",
		},
		{
			name:        "malformed rules file",
			rulesFile:   filepath.Join(dir, "malformed-rules.yaml"),
			writeRules:  "transformations: [",
			writeConfig: "",
		},
		{
			name:        "rules file without transformations",
			rulesFile:   filepath.Join(dir, "empty-rules.yaml"),
			writeRules:  "other: {}",
			writeConfig: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.writeConfig != "" {
				require.NoError(t, os.WriteFile(tc.configFile, []byte(tc.writeConfig), 0o600))
			}
			if tc.writeRules != "" {
				require.NoError(t, os.WriteFile(tc.rulesFile, []byte(tc.writeRules), 0o600))
			}

			load := NewTransformerConfigLoader(tc.configFile, tc.rulesFile)
			require.NotNil(t, load)

			cfg, err := load(context.Background())
			require.Error(t, err)
			require.Nil(t, cfg)
		})
	}
}
