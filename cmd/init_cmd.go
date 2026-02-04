// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/pkg/stream"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var initCmd = &cobra.Command{
	Use:    "init",
	Short:  "Initialises pgstream, creating the replication slot and the relevant tables/functions/triggers under the configured internal pgstream schema",
	PreRun: initDestroyFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("initialising pgstream...").Start()

		streamConfig, err := config.ParseStreamConfig()
		if err != nil {
			return fmt.Errorf("parsing stream config: %w", err)
		}

		initConfig := streamConfig.GetInitConfig(getInitOptions()...)

		if err := stream.Init(context.Background(), initConfig); err != nil {
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

var destroyCmd = &cobra.Command{
	Use:    "destroy",
	Short:  "It destroys any pgstream setup, removing the replication slot and all the relevant tables/functions/triggers, along with the internal pgstream schema",
	PreRun: initDestroyFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("destroying pgstream...").Start()

		streamConfig, err := config.ParseStreamConfig()
		if err != nil {
			return fmt.Errorf("parsing stream config: %w", err)
		}

		initConfig := streamConfig.GetInitConfig(getInitOptions()...)

		if err := stream.Destroy(context.Background(), initConfig); err != nil {
			sp.Fail(err.Error())
			return err
		}

		sp.Success("pgstream destroy complete")
		return nil
	},
	Example: `
	pgstream destroy --postgres-url <source-postgres-url> --replication-slot <replication-slot-name>
	pgstream destroy -c config.yaml
	pgstream destroy -c config.env`,
}

var tearDownCmd = &cobra.Command{
	Use:    "tear-down",
	Short:  "tear-down is deprecated, please use destroy",
	PreRun: initDestroyFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("tearing down pgstream...\ntear-down is deprecated, please use destroy").Start()

		streamConfig, err := config.ParseStreamConfig()
		if err != nil {
			return fmt.Errorf("parsing stream config: %w", err)
		}

		initConfig := streamConfig.GetInitConfig(getInitOptions()...)

		if err := stream.Destroy(context.Background(), initConfig); err != nil {
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

func initDestroyFlagBinding(cmd *cobra.Command, _ []string) {
	viper.BindPFlag("migrations-only", cmd.Flags().Lookup("migrations-only"))
	// to be able to overwrite configuration with flags when yaml config file is
	// provided
	viper.BindPFlag("source.postgres.url", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("source.postgres.replication.replication_slot", cmd.Flags().Lookup("replication-slot"))
	viper.Set("source.postgres.mode", "replication")
	viper.BindPFlag("modifiers.injector.enabled", cmd.Flags().Lookup("with-injector"))

	// to be able to overwrite configuration with flags when env config file is
	// provided or when no configuration is provided
	viper.BindPFlag("PGSTREAM_POSTGRES_LISTENER_URL", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME", cmd.Flags().Lookup("replication-slot"))
	if cmd.Flags().Lookup("with-injector").Value.String() == trueStr {
		viper.BindPFlag("PGSTREAM_INJECTOR_STORE_POSTGRES_URL", cmd.Flags().Lookup("postgres-url"))
	}
}

func getInitOptions() []stream.InitOption {
	initOpts := []stream.InitOption{}
	if viper.GetBool("migrations-only") {
		initOpts = append(initOpts, stream.WithMigrationsOnly())
	}
	return initOpts
}
