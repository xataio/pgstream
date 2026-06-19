// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgstream/cmd/config"
)

var checkCmd = &cobra.Command{
	Use:     "check",
	Short:   "Runs pre-migration checks to catch blocking issues before snapshot/run",
	PreRunE: checkFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("running pgstream checks...").Start()

		err := func() error {
			streamConfig, err := config.ParseStreamConfig()
			if err != nil {
				return fmt.Errorf("parsing stream config: %w", err)
			}
			_ = streamConfig

			// TODO: run checks. See https://github.com/xataio/pgstream/issues/897 for the
			// list of checks to implement.

			sp.Success("no checks to run")
			return nil
		}()
		if err != nil {
			sp.Fail(err.Error())
		}

		return err
	},
	Example: `
	pgstream check -c pg2pg.env
	pgstream check -c pg2pg.yaml --json
	`,
}

func checkFlagBinding(cmd *cobra.Command, _ []string) error {
	// to be able to overwrite configuration with flags when yaml config file is
	// provided
	viper.BindPFlag("source.postgres.url", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("target.postgres.url", cmd.Flags().Lookup("target-url"))

	// to be able to overwrite configuration with flags when env config file is
	// provided or when no configuration is provided
	viper.BindPFlag("PGSTREAM_POSTGRES_LISTENER_URL", cmd.Flags().Lookup("postgres-url"))
	viper.BindPFlag("PGSTREAM_POSTGRES_WRITER_TARGET_URL", cmd.Flags().Lookup("target-url"))
	return nil
}
