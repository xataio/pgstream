// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/pkg/stream"
)

var statusCmd = &cobra.Command{
	Use:    "status",
	Short:  "Checks the status of pgstream initialisation and provided configuration",
	PreRun: statusFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("checking pgstream status...").Start()

		streamConfig, err := config.ParseStreamConfig()
		if err != nil {
			return fmt.Errorf("parsing stream config: %w", err)
		}

		statusChecker := stream.NewStatusChecker()
		status, err := statusChecker.Status(context.Background(), streamConfig)
		if err != nil {
			sp.Fail(err.Error())
			return err
		}

		statusErrs := status.GetErrors()
		if len(statusErrs) == 0 {
			sp.Success("pgstream status check encountered no issues")
		} else {
			sp.Warning("pgstream status check identified issues with ", strings.Join(statusErrs.Keys(), ", "))
		}

		err = print(cmd, status)
		if err != nil {
			sp.Fail("failed to format pgstream status")
			return err
		}

		return nil
	},
	Example: `
	pgstream status -c pg2pg.env
	pgstream status --postgres-url <postgres-url> --replication-slot <replication-slot-name>
	pgstream status -c pg2pg.yaml --json
	`,
}

type printer interface {
	PrettyPrint() string
}

func print(cmd *cobra.Command, p printer) error {
	str := p.PrettyPrint()
	if cmd.Flags().Lookup("json").Value.String() == trueStr {
		var prettyJSON bytes.Buffer
		jsonData, err := json.Marshal(p)
		if err != nil {
			return err
		}
		if err := json.Indent(&prettyJSON, jsonData, "", "\t"); err != nil {
			return err
		}
		str = prettyJSON.String()
	}

	fmt.Println(str) //nolint:forbidigo
	return nil
}

func statusFlagBinding(cmd *cobra.Command, _ []string) {
	// to be able to overwrite configuration with flags when yaml config file is
	// provided
	viper.BindPFlag("source.postgres.url", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("source.postgres.replication.replication_slot", cmd.Flags().Lookup("replication-slot"))
	viper.Set("source.postgres.mode", "replication")

	// to be able to overwrite configuration with flags when env config file is
	// provided or when no configuration is provided
	viper.BindPFlag("PGSTREAM_POSTGRES_LISTENER_URL", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME", cmd.Flags().Lookup("replication-slot"))
}
