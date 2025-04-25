// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/internal/log/zerolog"
	"github.com/xataio/pgstream/pkg/stream"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Snapshot performs a snapshot of the configured source into the configured target",
	RunE:  withSignalWatcher(snapshot),
}

func snapshot(ctx context.Context) error {
	logger := zerolog.NewLogger(&zerolog.Config{
		LogLevel: viper.GetString("PGSTREAM_LOG_LEVEL"),
	})
	zerolog.SetGlobalLogger(logger)

	streamConfig, err := config.ParseStreamConfig()
	if err != nil {
		return err
	}
	return stream.Snapshot(ctx, zerolog.NewStdLogger(logger), streamConfig, nil)
}
