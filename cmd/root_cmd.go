// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgstream/cmd/config"
)

// Version is the pgstream version
var (
	Version = "development"
)

func Prepare() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "pgstream",
		SilenceUsage: true,
		Version:      Version,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return config.Load()
		},
	}

	viper.SetEnvPrefix("PGSTREAM")
	viper.AutomaticEnv()

	rootCmd.PersistentFlags().String("pgurl", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
	rootCmd.PersistentFlags().String("replication-slot", "", "Name of the postgres replication slot to be created")
	rootCmd.PersistentFlags().StringP("config", "c", "", ".env or .yaml config file to use with pgstream if any")
	rootCmd.PersistentFlags().String("log-level", "debug", "log level for the application. One of trace, debug, info, warn, error, fatal, panic")

	viper.BindPFlag("pgurl", rootCmd.PersistentFlags().Lookup("pgurl"))
	viper.BindPFlag("replication-slot", rootCmd.PersistentFlags().Lookup("replication-slot"))
	viper.BindPFlag("config", rootCmd.PersistentFlags().Lookup("config"))
	viper.BindPFlag("PGSTREAM_LOG_LEVEL", rootCmd.PersistentFlags().Lookup("log-level"))

	// register subcommands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(tearDownCmd)
	rootCmd.AddCommand(runCmd)

	return rootCmd
}

// Execute executes the root command.
func Execute() error {
	cmd := Prepare()
	return cmd.Execute()
}

func withSignalWatcher(fn func(ctx context.Context) error) func(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sigc
		cancel()
	}()

	return func(cmd *cobra.Command, args []string) error {
		defer cancel()
		return fn(ctx)
	}
}
