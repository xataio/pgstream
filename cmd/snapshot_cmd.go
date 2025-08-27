// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/internal/log/zerolog"
	"github.com/xataio/pgstream/pkg/stream"
)

var snapshotCmd = &cobra.Command{
	Use:     "snapshot",
	Short:   "Snapshot performs a snapshot of the configured source Postgres database into the configured target",
	PreRunE: snapshotFlagBinding,
	RunE:    withProfiling(withSignalWatcher(snapshot)),
	Example: `
	pgstream snapshot --postgres-url <postgres-url> --target postgres --target-url <target-url> --tables <schema.table> --reset
	pgstream snapshot --config config.yaml --log-level info
	pgstream snapshot --config config.env`,
}

func snapshot(ctx context.Context) error {
	logger := zerolog.NewLogger(&zerolog.Config{
		LogLevel: viper.GetString("PGSTREAM_LOG_LEVEL"),
	})
	zerolog.SetGlobalLogger(logger)

	streamConfig, err := config.ParseStreamConfig()
	if err != nil {
		return fmt.Errorf("parsing stream config: %w", err)
	}

	provider, err := newInstrumentationProvider()
	if err != nil {
		return err
	}
	defer provider.Close()

	return stream.Snapshot(ctx, zerolog.NewStdLogger(logger), streamConfig, provider.NewInstrumentation("snapshot"))
}

func snapshotFlagBinding(cmd *cobra.Command, args []string) error {
	// to be able to overwrite configuration with flags when yaml config file is
	// provided
	viper.BindPFlag("source.postgres.url", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("source.postgres.snapshot.tables", cmd.Flags().Lookup("tables"))
	viper.BindPFlag("source.postgres.snapshot.schema.pgdump_pgrestore.clean_target_db", cmd.Flags().Lookup("reset"))
	viper.BindPFlag("source.postgres.snapshot.schema.pgdump_pgrestore.roles_snapshot_mode", cmd.Flags().Lookup("roles-snapshot-mode"))
	viper.BindPFlag("source.postgres.snapshot.schema.pgdump_pgrestore.dump_file", cmd.Flags().Lookup("dump-file"))
	// if not explicitly set, default to repeatable for snapshot command
	if viper.GetString("source.postgres.snapshot.recorder.repeatable_snapshots") == "" {
		viper.Set("source.postgres.snapshot.recorder.repeatable_snapshots", true)
	}

	// if the target is postgres, default to using bulk ingest if not set
	if viper.GetString("target.postgres.url") != "" && viper.GetString("target.postgres.bulk_ingest.enabled") == "" {
		viper.Set("target.postgres.bulk_ingest.enabled", true)
	}

	// to be able to overwrite configuration with flags when env config file is
	// provided or when no configuration is provided
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_TABLES", cmd.Flags().Lookup("tables"))
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB", cmd.Flags().Lookup("reset"))
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_ROLES_SNAPSHOT_MODE", cmd.Flags().Lookup("roles-snapshot-mode"))
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_DUMP_FILE", cmd.Flags().Lookup("dump-file"))
	// if not explicitly set, default to repeatable for snapshot command
	if viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_STORE_REPEATABLE") == "" {
		viper.Set("PGSTREAM_POSTGRES_SNAPSHOT_STORE_REPEATABLE", true)
	}

	// if the target is postgres, default to using bulk ingest if not set
	if viper.GetString("PGSTREAM_POSTGRES_WRITER_TARGET_URL") != "" &&
		viper.GetString("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED") == "" {
		viper.Set("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED", true)
	}

	return targetFlagBinding(cmd)
}
