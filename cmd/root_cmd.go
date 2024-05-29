// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Version is the pgstream version
var (
	Version = "development"
	env     *Env
)

func init() {
	viper.SetEnvPrefix("PGSTREAM")
	viper.AutomaticEnv()

	env = loadEnv()
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
	rootCmd.AddCommand(pg2kafkaCmd)
	rootCmd.AddCommand(kafka2osCmd)

	return rootCmd.Execute()
}
