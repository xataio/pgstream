// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

var (
	viperReadRegex     = regexp.MustCompile(`viper\.Get\w*\("(PGSTREAM_[A-Z0-9_]+)"\)`)
	viperBindEnvRegex  = regexp.MustCompile(`viper\.BindEnv\("(PGSTREAM_[A-Z0-9_]+)"\)`)
	errUnboundEnvVarFn = "environment variables read by cmd/config but never registered in bindEnvVars, so exporting them has no effect"
)

// Test_bindEnvVars_readKeysAreBound guards a class of silent misconfiguration
// rather than a single variable. viper resolves an environment variable only
// for the keys bound with BindEnv: the AutomaticEnv fallback the root command
// installs looks each key up under a second PGSTREAM_ prefix, so it never
// matches these names, and a key that is read but not bound is ignored with no
// error anywhere. The keys are scanned out of the source because the read and
// the registration sit in different places and nothing else ties them together.
//
// Keys assembled at runtime (the backoff and TLS helpers build theirs from a
// prefix) are not matched by the scan and are not covered.
func Test_bindEnvVars_readKeysAreBound(t *testing.T) {
	readKeys := map[string]string{}
	boundKeys := map[string]struct{}{}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		src, err := os.ReadFile(name)
		require.NoError(t, err)
		for _, match := range viperReadRegex.FindAllStringSubmatch(string(src), -1) {
			readKeys[match[1]] = name
		}
		for _, match := range viperBindEnvRegex.FindAllStringSubmatch(string(src), -1) {
			boundKeys[match[1]] = struct{}{}
		}
	}

	// a scan that finds nothing would pass without checking anything
	require.NotEmpty(t, readKeys)
	require.NotEmpty(t, boundKeys)

	unbound := []string{}
	for key, file := range readKeys {
		if _, found := boundKeys[key]; !found {
			unbound = append(unbound, key+" (read in "+file+")")
		}
	}
	slices.Sort(unbound)
	require.Empty(t, unbound, errUnboundEnvVarFn)
}

// Test_bindEnvVars_resolvesFromTheEnvironment covers what the scan above
// cannot: that a registered key is actually resolved from the process
// environment. The other tests in this package share viper's global config
// map with the ones that load test_config.env, so a value can appear to come
// from the environment while it is really being read out of that file.
func Test_bindEnvVars_resolvesFromTheEnvironment(t *testing.T) {
	reset := func() {
		viper.Reset()
		bindEnvVars()
	}
	reset()
	t.Cleanup(reset)

	t.Setenv("PGSTREAM_POSTGRES_SNAPSHOT_ROLE", "a-role-from-the-environment")
	require.Equal(t, "a-role-from-the-environment", viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_ROLE"))
}
