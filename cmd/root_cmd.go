// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Version is the pgstream version
var Version = "development"

func init() {
	viper.SetEnvPrefix("PGSTREAM")
	viper.AutomaticEnv()

	rootCmd.PersistentFlags().String("pgurl", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
	viper.BindPFlag("pgurl", rootCmd.PersistentFlags().Lookup("pgurl"))
}

var rootCmd = &cobra.Command{
	Use:          "pgstream",
	SilenceUsage: true,
	Version:      Version,
}

// Execute executes the root command.
func Execute() error {
	// register subcommands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(tearDownCmd)

	return rootCmd.Execute()
}
