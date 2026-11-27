// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	rszerolog "github.com/rs/zerolog"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/log/zerolog"
)

func writeSIGHUPReloadTestConfig(t *testing.T, level string) {
	t.Helper()

	configFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("logging:\n  level: "+level+"\n  format:\n    type: json\n"), 0o600))
	viper.Set("config", configFile)
}

func TestHandleSIGHUPReload_RequestsTransformationReload(t *testing.T) {
	t.Cleanup(viper.Reset)
	t.Cleanup(func() { rszerolog.SetGlobalLevel(rszerolog.TraceLevel) })
	viper.Reset()

	writeSIGHUPReloadTestConfig(t, "warn")
	logger := zerolog.NewLogger(&zerolog.Config{LogLevel: "debug", LogFormat: "json"})
	requests := make(chan struct{}, 1)

	handleSIGHUPReload(logger, requests)

	require.Equal(t, rszerolog.WarnLevel, rszerolog.GlobalLevel())
	require.Len(t, requests, 1)
}

func TestHandleSIGHUPReload_InvalidLogLevelStillRequestsTransformationReload(t *testing.T) {
	t.Cleanup(viper.Reset)
	t.Cleanup(func() { rszerolog.SetGlobalLevel(rszerolog.TraceLevel) })
	viper.Reset()

	writeSIGHUPReloadTestConfig(t, "verbose")
	logger := zerolog.NewLogger(&zerolog.Config{LogLevel: "debug", LogFormat: "json"})
	requests := make(chan struct{}, 1)

	handleSIGHUPReload(logger, requests)

	require.Equal(t, rszerolog.DebugLevel, rszerolog.GlobalLevel())
	require.Len(t, requests, 1)
}

func TestHandleSIGHUPReload_FullRequestChannelDoesNotBlock(t *testing.T) {
	t.Cleanup(viper.Reset)
	t.Cleanup(func() { rszerolog.SetGlobalLevel(rszerolog.TraceLevel) })
	viper.Reset()

	writeSIGHUPReloadTestConfig(t, "warn")
	logger := zerolog.NewLogger(&zerolog.Config{LogLevel: "debug", LogFormat: "json"})
	requests := make(chan struct{}, 1)
	requests <- struct{}{}

	done := make(chan struct{})
	go func() {
		handleSIGHUPReload(logger, requests)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SIGHUP reload blocked on a full transformation request channel")
	}
}

func TestHandleSIGHUPReload_NilRequestChannel(t *testing.T) {
	t.Cleanup(viper.Reset)
	t.Cleanup(func() { rszerolog.SetGlobalLevel(rszerolog.TraceLevel) })
	viper.Reset()

	writeSIGHUPReloadTestConfig(t, "warn")
	logger := zerolog.NewLogger(&zerolog.Config{LogLevel: "debug", LogFormat: "json"})

	require.NotPanics(t, func() {
		handleSIGHUPReload(logger, nil)
	})
}
