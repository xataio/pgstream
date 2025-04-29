// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/pkg/stream"
)

var initStatusCmd = &cobra.Command{
	Use:    "status",
	Short:  "Checks the initialisation status of pgstream in the provided postgres database",
	PreRun: statusFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("checking pgstream initialisation status...").Start()

		streamConfig, err := config.ParseStreamConfig()
		if err != nil {
			return err
		}

		statusChecker := stream.NewStatusChecker()
		status, err := statusChecker.InitStatus(context.Background(), streamConfig.SourcePostgresURL(), streamConfig.PostgresReplicationSlot())
		if err != nil {
			sp.Fail(err.Error())
			return err
		}

		statusErrs := status.GetErrors()
		if len(statusErrs) == 0 {
			sp.Success("pgstream initialisation status check encountered no issues")
		} else {
			sp.Warning("pgstream initialisation status check identified issues: \n", strings.Join(statusErrs, "\n"))
		}

		statusJSON, _ := json.Marshal(status)
		sp.Info("pgstream initialisation status: ", string(statusJSON))

		return nil
	},
}

func statusFlagBinding(cmd *cobra.Command, _ []string) {
	// to be able to overwrite configuration with flags when yaml config file is
	// provided
	viper.BindPFlag("source.postgres.url", cmd.InheritedFlags().Lookup("postgres-url"))
	viper.BindPFlag("source.postgres.replication.replication_slot", cmd.InheritedFlags().Lookup("replication-slot"))
	viper.Set("source.postgres.mode", "replication")

	// to be able to overwrite configuration with flags when env config file is
	// provided or when no configuration is provided
	viper.BindPFlag("PGSTREAM_POSTGRES_LISTENER_URL", cmd.InheritedFlags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL", cmd.InheritedFlags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME", cmd.InheritedFlags().Lookup("replication-slot"))
}
