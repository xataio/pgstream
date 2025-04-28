// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/pkg/stream"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var initCmd = &cobra.Command{
	Use:    "init",
	Short:  "Initialises pgstream, creating the replication slot and the relevant tables/functions/triggers under the configured internal pgstream schema.",
	PreRun: initTeardownFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("initialising pgstream...").Start()

		streamConfig, err := config.ParseStreamConfig()
		if err != nil {
			return err
		}

		if err := stream.Init(context.Background(), streamConfig.SourcePostgresURL(), streamConfig.PostgresReplicationSlot()); err != nil {
			sp.Fail(err.Error())
			return err
		}

		sp.Success("pgstream initialisation complete")
		return nil
	},
	Example: `
	pgstream init --postgres-url <source-postgres-url> --replication-slot <replication-slot-name>
	pgstream init -c config.yaml
	pgstream init -c config.env`,
}

var tearDownCmd = &cobra.Command{
	Use:    "tear-down",
	Short:  "It tears down any pgstream setup, removing the replication slot and all the relevant tables/functions/triggers, along with the internal pgstream schema.",
	PreRun: initTeardownFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("tearing down pgstream...").Start()

		streamConfig, err := config.ParseStreamConfig()
		if err != nil {
			return err
		}

		if err := stream.TearDown(context.Background(), streamConfig.SourcePostgresURL(), streamConfig.PostgresReplicationSlot()); err != nil {
			sp.Fail(err.Error())
			return err
		}

		sp.Success("pgstream tear down complete")
		return nil
	},
	Example: `
	pgstream tear-down --postgres-url <source-postgres-url> --replication-slot <replication-slot-name>
	pgstream tear-down -c config.yaml
	pgstream tear-down -c config.env`,
}

func initTeardownFlagBinding(cmd *cobra.Command, _ []string) {
	// to be able to overwrite configuration with flags when yaml config file is
	// provided
	viper.BindPFlag("source.postgres.url", cmd.PersistentFlags().Lookup("postgres-url"))
	viper.BindPFlag("source.postgres.replication.replication_slot", cmd.PersistentFlags().Lookup("replication-slot"))
	viper.Set("source.postgres.mode", "replication")

	// to be able to overwrite configuration with flags when env config file is
	// provided or when no configuration is provided
	viper.BindPFlag("PGSTREAM_POSTGRES_LISTENER_URL", cmd.PersistentFlags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL", cmd.PersistentFlags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME", cmd.PersistentFlags().Lookup("replication-slot"))
}
