// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	rszerolog "github.com/rs/zerolog"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/log/zerolog"
)

func TestReloadLogLevel(t *testing.T) {
	t.Cleanup(viper.Reset)
	viper.Reset()

	configFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("logging:\n  level: warn\n  format:\n    type: json\n"), 0o600))
	viper.Set("config", configFile)

	logger := zerolog.NewLogger(&zerolog.Config{LogLevel: "debug", LogFormat: "json"})
	require.Equal(t, rszerolog.DebugLevel, logger.GetLevel())

	require.NoError(t, reloadLogLevel(logger))
	require.Equal(t, rszerolog.WarnLevel, logger.GetLevel())
}

func TestReloadLogLevelKeepsPreviousLevelOnInvalidConfig(t *testing.T) {
	t.Cleanup(viper.Reset)
	viper.Reset()

	configFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("logging:\n  level: warn\n  format:\n    type: json\n"), 0o600))
	viper.Set("config", configFile)

	logger := zerolog.NewLogger(&zerolog.Config{LogLevel: "debug", LogFormat: "json"})
	require.NoError(t, reloadLogLevel(logger))
	require.Equal(t, rszerolog.WarnLevel, logger.GetLevel())

	require.NoError(t, os.WriteFile(configFile, []byte("logging:\n  level: verbose\n  format:\n    type: json\n"), 0o600))
	err := reloadLogLevel(logger)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid log level")
	require.Equal(t, rszerolog.WarnLevel, logger.GetLevel())
}
