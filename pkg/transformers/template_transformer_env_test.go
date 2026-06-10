// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// sha256hex mirrors what sprig's `sha256sum` does: hex-encoded SHA-256.
func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestTemplateTransformer_SprigEnvSalt verifies that the sprig `env` function is
// available inside the template transformer and can be used to inject a salt
// from an environment variable, e.g.:
//
//	template: '{{ printf "%s%s" (env "MY_SALT") .GetValue | sha256sum }}'
func TestTemplateTransformer_SprigEnvSalt(t *testing.T) {
	const tmpl = `{{ printf "%s%s" (env "MY_SALT") .GetValue | sha256sum }}`

	t.Run("salt from env var is applied", func(t *testing.T) {
		t.Setenv("MY_SALT", "abc123")

		tr, err := NewTemplateTransformer(ParameterValues{"template": tmpl})
		require.NoError(t, err)

		got, err := tr.Transform(context.Background(), Value{TransformValue: "hello"})
		require.NoError(t, err)

		// The output is the hex SHA-256 of salt + value.
		require.Equal(t, sha256hex("abc123hello"), got)
	})

	t.Run("changing the salt changes the hash", func(t *testing.T) {
		t.Setenv("MY_SALT", "different-salt")

		tr, err := NewTemplateTransformer(ParameterValues{"template": tmpl})
		require.NoError(t, err)

		got, err := tr.Transform(context.Background(), Value{TransformValue: "hello"})
		require.NoError(t, err)

		require.Equal(t, sha256hex("different-salthello"), got)
		require.NotEqual(t, sha256hex("abc123hello"), got)
	})

	// Footgun: sprig's `env` is just os.Getenv, so an unset (or misspelled)
	// variable silently becomes an empty string and you hash with no salt.
	t.Run("unset env var silently hashes with empty salt", func(t *testing.T) {
		t.Setenv("MY_SALT", "")

		tr, err := NewTemplateTransformer(ParameterValues{"template": tmpl})
		require.NoError(t, err)

		got, err := tr.Transform(context.Background(), Value{TransformValue: "hello"})
		require.NoError(t, err)

		// No error, no salt — equal to hashing the bare value.
		require.Equal(t, sha256hex("hello"), got)
	})

	// Optional guard: use sprig's `fail` to turn a missing salt into a loud
	// error instead of a silent empty-salt hash.
	t.Run("fail guard errors when salt is missing", func(t *testing.T) {
		t.Setenv("MY_SALT", "")

		const guarded = `{{ $salt := env "MY_SALT" }}` +
			`{{ if not $salt }}{{ fail "MY_SALT is not set" }}{{ end }}` +
			`{{ printf "%s%s" $salt .GetValue | sha256sum }}`

		tr, err := NewTemplateTransformer(ParameterValues{"template": guarded})
		require.NoError(t, err)

		_, err = tr.Transform(context.Background(), Value{TransformValue: "hello"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "MY_SALT is not set")
	})
}
