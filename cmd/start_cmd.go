// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xataio/pgstream/internal/log/zerolog"
	"github.com/xataio/pgstream/pkg/stream"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the configured pgstream modules",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := zerolog.NewLogger(&zerolog.Config{
			LogLevel: viper.GetString("PGSTREAM_LOG_LEVEL"),
		})
		zerolog.SetGlobalLogger(logger)
		return stream.Start(context.Background(), zerolog.NewStdLogger(logger), parseStreamConfig())
	},
}
