// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Version is the pgstream version
var (
	Version = "development"
)

func init() {
	viper.SetEnvPrefix("PGSTREAM")
	viper.AutomaticEnv()

	rootCmd.PersistentFlags().String("pgurl", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
	rootCmd.PersistentFlags().StringP("config", "c", "", ".env config file to use if any")
	rootCmd.PersistentFlags().String("log-level", "debug", "log level for the application")

	viper.BindPFlag("pgurl", rootCmd.PersistentFlags().Lookup("pgurl"))
	viper.BindPFlag("config", rootCmd.PersistentFlags().Lookup("config"))
	viper.BindPFlag("PGSTREAM_LOG_LEVEL", rootCmd.PersistentFlags().Lookup("log-level"))
}

var rootCmd = &cobra.Command{
	Use:          "pgstream",
	SilenceUsage: true,
	Version:      Version,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return loadConfig()
	},
}

// Execute executes the root command.
func Execute() error {
	// register subcommands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(tearDownCmd)
	rootCmd.AddCommand(startCmd)

	return rootCmd.Execute()
}
