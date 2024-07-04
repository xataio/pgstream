// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xataio/pgstream/internal/log/zerolog"
	"github.com/xataio/pgstream/pkg/stream"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run starts all the configured pgstream modules",
	RunE:  withSignalWatcher(run),
}

func run(ctx context.Context) error {
	logger := zerolog.NewLogger(&zerolog.Config{
		LogLevel: viper.GetString("PGSTREAM_LOG_LEVEL"),
	})
	zerolog.SetGlobalLogger(logger)
	return stream.Run(ctx, zerolog.NewStdLogger(logger), parseStreamConfig(), nil)
}
