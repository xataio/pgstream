// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"io"
	"testing"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"
)

func TestApplyLogLevelFromConfigUpdatesLevel(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("logging.level", "error")

	logger := zerolog.New(io.Discard).Level(zerolog.InfoLevel)

	applyLogLevelFromConfig(&logger)

	if got := logger.GetLevel(); got != zerolog.ErrorLevel {
		t.Fatalf("expected log level %q, got %q", zerolog.ErrorLevel, got)
	}
}

func TestApplyLogLevelFromConfigRejectsInvalidLevel(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("logging.level", "bad-level")

	logger := zerolog.New(io.Discard).Level(zerolog.InfoLevel)

	applyLogLevelFromConfig(&logger)

	if got := logger.GetLevel(); got != zerolog.InfoLevel {
		t.Fatalf("expected log level to remain %q, got %q", zerolog.InfoLevel, got)
	}
}
