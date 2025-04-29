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
	Env     string
)

func Prepare() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "pgstream",
		SilenceUsage: true,
		Version:      version(),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return config.Load()
		},
	}

	viper.SetEnvPrefix("PGSTREAM")
	viper.AutomaticEnv()

	// Flag definition

	// root cmd
	rootCmd.PersistentFlags().StringP("config", "c", "", ".env or .yaml config file to use with pgstream if any")
	rootCmd.PersistentFlags().String("log-level", "debug", "log level for the application. One of trace, debug, info, warn, error, fatal, panic")

	// init cmd
	initCmd.PersistentFlags().String("postgres-url", "", "Source postgres URL where pgstream setup will be run")
	initCmd.PersistentFlags().String("replication-slot", "", "Name of the postgres replication slot to be created by pgstream on the source url")

	// tear down cmd
	tearDownCmd.Flags().String("postgres-url", "", "Source postgres URL where pgstream tear down will be run")
	tearDownCmd.Flags().String("replication-slot", "", "Name of the postgres replication slot to be deleted by pgstream from the source url")

	// snapshot cmd
	snapshotCmd.Flags().String("postgres-url", "", "Source postgres database to perform the snapshot from")
	snapshotCmd.Flags().String("target", "", "Target type. One of postgres, opensearch, elasticsearch, kafka")
	snapshotCmd.Flags().String("target-url", "", "Target URL")
	snapshotCmd.Flags().StringSlice("tables", nil, "List of tables to snapshot, in the format <schema>.<table>. If not specified, the schema `public` will be assumed. Wildcards are supported")
	snapshotCmd.Flags().Bool("reset", false, "Wether to reset the target before snapshotting (only for postgres target)")

	// run cmd
	runCmd.Flags().String("source", "", "Source type. One of postgres, kafka")
	runCmd.Flags().String("source-url", "", "Source URL")
	runCmd.Flags().String("target", "", "Target type. One of postgres, opensearch, elasticsearch, kafka")
	runCmd.Flags().String("target-url", "", "Target URL")
	runCmd.Flags().String("replication-slot", "", "Name of the postgres replication slot for pgstream to connect to")
	runCmd.Flags().StringSlice("snapshot-tables", nil, "List of tables to snapshot if initial snapshot is required, in the format <schema>.<table>. If not specified, the schema `public` will be assumed. Wildcards are supported")
	runCmd.Flags().Bool("reset", false, "Wether to reset the target before snapshotting (only for postgres target)")

	// Flag binding for root cmd
	rootFlagBinding(rootCmd)

	// register subcommands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(tearDownCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(snapshotCmd)
	initCmd.AddCommand(initStatusCmd)
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

func rootFlagBinding(cmd *cobra.Command) {
	viper.BindPFlag("config", cmd.PersistentFlags().Lookup("config"))
	viper.BindPFlag("PGSTREAM_LOG_LEVEL", cmd.PersistentFlags().Lookup("log-level"))
}

func version() string {
	if Env != "" {
		return Env + " (" + Version + ")"
	}
	return Version
}
