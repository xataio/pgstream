// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/pkg/stream"
)

// parent command for validation subcommands
var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate different parts of the pgstream configuration",
}

var errNoPostgresURL = errors.New("postgres URL is required for transformation rules validation")

var validateRulesCmd = &cobra.Command{
	Use:     "rules",
	Short:   "Validates transformation rules against the provided Postgres database",
	PreRunE: validateRulesFlagBinding,
	RunE: func(cmd *cobra.Command, args []string) error {
		sp, _ := pterm.DefaultSpinner.WithText("validating pgstream transformation rules...").Start()

		err := func() error {
			streamConfig, err := config.ParseStreamConfig()
			if err != nil {
				return fmt.Errorf("parsing stream config: %w", err)
			}

			if streamConfig.Listener.Postgres == nil || streamConfig.Listener.Postgres.URL == "" {
				return errNoPostgresURL
			}

			if streamConfig.Processor.Transformer.HasNoRules() {
				// If yaml config file is provided without rules, make sure to
				// overwrite with rules file from flag if available.
				if cmd.Flags().Lookup("rules-file").Changed {
					rulesConfig, err := config.ParseTransformerConfig(cmd.Flags().Lookup("rules-file").Value.String())
					if err != nil {
						return fmt.Errorf("parsing transformer rules config: %w", err)
					}
					streamConfig.Processor.Transformer = rulesConfig
				}

				// check again after overwriting rules
				if streamConfig.Processor.Transformer.HasNoRules() {
					sp.Success("no transformation rules to validate")
					return nil
				}
			}

			statusChecker := stream.NewStatusChecker()
			rulesStatus, err := statusChecker.TransformationRulesStatus(context.Background(), streamConfig)
			if err != nil {
				return err
			}

			if len(rulesStatus.Errors) == 0 {
				sp.Success("transformation rules are valid")
			} else {
				sp.Warning("pgstream validation check identified issues: ", strings.Join(rulesStatus.Errors, ", "))
			}

			err = print(cmd, rulesStatus)
			if err != nil {
				return fmt.Errorf("failed to format pgstream validation status: %w", err)
			}

			return nil
		}()
		if err != nil {
			sp.Fail(err.Error())
		}

		return err
	},
	Example: `
	pgstream validate rules -c pg2pg.env
	pgstream validate rules --postgres-url <postgres-url> --rules-file rules.yaml
	pgstream validate rules -c pg2pg.yaml --json
	`,
}

func validateRulesFlagBinding(cmd *cobra.Command, _ []string) error {
	// to be able to overwrite configuration with flags when yaml config file is
	// provided
	viper.BindPFlag("source.postgres.url", cmd.Flags().Lookup("postgres-url"))

	// to be able to overwrite configuration with flags when env config file is
	// provided or when no configuration is provided
	viper.BindPFlag("PGSTREAM_TRANSFORMER_RULES_FILE", cmd.Flags().Lookup("rules-file"))
	viper.BindPFlag("PGSTREAM_POSTGRES_LISTENER_URL", cmd.Flags().Lookup("postgres-url"))
	return nil
}
