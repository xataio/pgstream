// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/stream/preflight"
)

var errCheckFailed = errors.New("checks reported errors")

// categoryBuilder turns a stream.Config into the concrete checks for a
// category. Each new category adds an entry to categoryBuilders and a matching
// CLI flag in root_cmd.go.
type categoryBuilder struct {
	category preflight.Category
	flag     string
	build    func(*stream.Config) []preflight.Check
}

var categoryBuilders = []categoryBuilder{
	{preflight.CategoryConnectivity, "connectivity", buildConnectivityChecks},
}

func buildConnectivityChecks(cfg *stream.Config) []preflight.Check {
	checks := []preflight.Check{}
	if url := cfg.SourcePostgresURL(); url != "" {
		checks = append(checks, &preflight.ConnectivityCheck{Label: "source", URL: url})
	}
	if cfg.Processor.Postgres != nil {
		if url := cfg.Processor.Postgres.BatchWriter.URL; url != "" {
			checks = append(checks, &preflight.ConnectivityCheck{Label: "target", URL: url})
		}
	}
	return checks
}

// selectedCategories returns the categories whose CLI flag was set to true.
// If no category flag was set, every registered category is selected.
func selectedCategories(cmd *cobra.Command) []preflight.Category {
	selected := []preflight.Category{}
	for _, b := range categoryBuilders {
		on, err := cmd.Flags().GetBool(b.flag)
		if err == nil && on {
			selected = append(selected, b.category)
		}
	}
	if len(selected) == 0 {
		for _, b := range categoryBuilders {
			selected = append(selected, b.category)
		}
	}
	return selected
}

func buildChecks(cfg *stream.Config, selected []preflight.Category) []preflight.Check {
	want := make(map[preflight.Category]bool, len(selected))
	for _, c := range selected {
		want[c] = true
	}
	checks := []preflight.Check{}
	for _, b := range categoryBuilders {
		if want[b.category] {
			checks = append(checks, b.build(cfg)...)
		}
	}
	return checks
}

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

			checks := buildChecks(streamConfig, selectedCategories(cmd))
			if len(checks) == 0 {
				sp.Success("no checks to run")
				return nil
			}

			report := preflight.Run(context.Background(), checks, preflight.WithProgress(func(idx, total int, name string) {
				sp.UpdateText(fmt.Sprintf("running %d/%d checks: %s", idx, total, name))
			}))

			if report.HasErrors() {
				sp.Stop()
			} else {
				sp.Success("pgstream checks passed")
			}

			if err := print(cmd, report); err != nil {
				return fmt.Errorf("failed to format check report: %w", err)
			}

			if report.HasErrors() {
				return errCheckFailed
			}
			return nil
		}()
		if err != nil && !errors.Is(err, errCheckFailed) {
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
