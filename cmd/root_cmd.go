// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/internal/log/zerolog"
	"github.com/xataio/pgstream/internal/profiling"
	"github.com/xataio/pgstream/pkg/otel"
)

// Version is the pgstream version
var (
	Version = "development"
	Env     string
)

const trueStr = "true"

func Prepare() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "pgstream",
		SilenceUsage: true,
		Version:      version(),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := config.Load(); err != nil {
				return fmt.Errorf("loading configuration: %w", err)
			}

			if err := loggerConfigFromViper().Validate(); err != nil {
				return err
			}

			return nil
		},
	}

	viper.SetEnvPrefix("PGSTREAM")
	viper.AutomaticEnv()

	// Flag definition

	// root cmd
	rootCmd.PersistentFlags().StringP("config", "c", "", ".env or .yaml config file to use with pgstream if any")
	rootCmd.PersistentFlags().String("log-level", "debug", "log level for the application. One of trace, debug, info, warn, error, fatal, panic")
	rootCmd.PersistentFlags().String("log-format", "console", "log output format. One of console, json")
	rootCmd.PersistentFlags().Bool("no-color", false, "disable ANSI color codes in console log output (ignored when --log-format=json)")

	// init cmd
	initCmd.Flags().String("postgres-url", "", "Source postgres URL where pgstream setup will be run")
	initCmd.Flags().String("replication-slot", "", "Name of the postgres replication slot to be created by pgstream on the source url")
	initCmd.Flags().Bool("with-injector", false, "Whether to initialise pgstream with the injector database migrations")
	initCmd.Flags().Bool("migrations-only", false, "Whether to only run the initialization database migrations")

	// destroy cmd
	destroyCmd.Flags().String("postgres-url", "", "Source postgres URL where pgstream destroy will be run")
	destroyCmd.Flags().String("replication-slot", "", "Name of the postgres replication slot to be deleted by pgstream from the source url")
	destroyCmd.Flags().Bool("with-injector", false, "Whether to also destroy the injector related database objects")
	destroyCmd.Flags().Bool("migrations-only", false, "Whether to only revert the database migrations")

	// tear down cmd
	tearDownCmd.Flags().String("postgres-url", "", "Source postgres URL where pgstream tear down will be run")
	tearDownCmd.Flags().String("replication-slot", "", "Name of the postgres replication slot to be deleted by pgstream from the source url")
	tearDownCmd.Flags().Bool("with-injector", false, "Whether to also tear down the injector related database objects")
	tearDownCmd.Flags().Bool("migrations-only", false, "Whether to only revert the database migrations")

	// snapshot cmd
	snapshotCmd.Flags().String("postgres-url", "", "Source postgres database to perform the snapshot from")
	snapshotCmd.Flags().String("target", "", "Target type. One of postgres, opensearch, elasticsearch, kafka")
	snapshotCmd.Flags().String("target-url", "", "Target URL")
	snapshotCmd.Flags().StringSlice("tables", nil, "List of tables to snapshot, in the format <schema>.<table>. If not specified, the schema `public` will be assumed. Wildcards are supported")
	snapshotCmd.Flags().Bool("reset", false, "Whether to reset the target before snapshotting (only for postgres target)")
	snapshotCmd.Flags().Bool("profile", false, "Whether to produce CPU and memory profile files, as well as exposing a /debug/pprof endpoint on localhost:6060")
	snapshotCmd.Flags().String("dump-file", "", "File where the pg_dump output will be written")

	// run cmd
	runCmd.Flags().String("source", "", "Source type. One of postgres, kafka")
	runCmd.Flags().String("source-url", "", "Source URL")
	runCmd.Flags().String("target", "", "Target type. One of postgres, opensearch, elasticsearch, kafka")
	runCmd.Flags().String("target-url", "", "Target URL")
	runCmd.Flags().String("replication-slot", "", "Name of the postgres replication slot for pgstream to connect to")
	runCmd.Flags().StringSlice("snapshot-tables", nil, "List of tables to snapshot if initial snapshot is required, in the format <schema>.<table>. If not specified, the schema `public` will be assumed. Wildcards are supported")
	runCmd.Flags().Bool("reset", false, "Whether to reset the target before snapshotting (only for postgres target)")
	runCmd.Flags().Bool("profile", false, "Whether to expose a /debug/pprof endpoint on localhost:6060")
	runCmd.Flags().BoolVar(&initFlag, "init", false, "Whether to initialize pgstream before starting replication")
	runCmd.Flags().String("dump-file", "", "File where the pg_dump output will be written if initial snapshot is enabled")
	runCmd.Flags().Bool("data-only", false, "When used with --snapshot-tables, skip schema restore and only snapshot data (use when schema is already present on target)")
	runCmd.Flags().Bool("with-injector", false, "Whether to enable the injection of pgstream metadata to the WAL events. Required for search targets.")

	// status cmd
	statusCmd.Flags().String("postgres-url", "", "Source postgres URL where pgstream has been initialised")
	statusCmd.Flags().String("replication-slot", "", "Name of the postgres replication slot created by pgstream on the source url")
	statusCmd.Flags().Bool("json", false, "Output the status in JSON format")

	// validate cmd
	// validate rules cmd
	validateRulesCmd.Flags().String("postgres-url", "", "Source postgres URL to validate the rules against")
	validateRulesCmd.Flags().StringP("rules-file", "f", "", "Path to a YAML file containing the transformation rules to validate")
	validateRulesCmd.Flags().Bool("json", false, "Output the validation status in JSON format")
	validateCmd.AddCommand(validateRulesCmd)

	// Flag binding for root cmd
	rootFlagBinding(rootCmd)

	// register subcommands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(destroyCmd)
	rootCmd.AddCommand(tearDownCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(validateCmd)
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

func withProfiling(fn func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) (err error) {
	return func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Lookup("profile").Value.String() == "false" {
			return fn(cmd, args)
		}

		profiling.StartProfilingServer("localhost:6060")
		// run is a long running process, do not produce a cpu/mem files but
		// rather expose the http endpoint only.
		if cmd.Name() == "run" {
			return fn(cmd, args)
		}

		stopCPUProfile, err := profiling.StartCPUProfile("cpu.prof")
		if err != nil {
			return err
		}
		defer func() {
			stopCPUProfile()
			profiling.CreateMemoryProfile("mem.prof")
		}()

		return fn(cmd, args)
	}
}

func loggerConfigFromViper() *zerolog.Config {
	return &zerolog.Config{
		LogLevel:  resolveLogString("PGSTREAM_LOG_LEVEL", "logging.level"),
		LogFormat: resolveLogString("PGSTREAM_LOG_FORMAT", "logging.format.type"),
		NoColor:   resolveLogBool("PGSTREAM_LOG_NO_COLOR", "logging.format.no_color"),
	}
}

// resolveLogString picks the value from the flag/env-style key when it was set
// explicitly (CLI flag changed or env var present), otherwise it falls back to
// the YAML key, and finally to the flag default.
func resolveLogString(envKey, yamlKey string) string {
	if viper.IsSet(envKey) {
		return viper.GetString(envKey)
	}
	if v := viper.GetString(yamlKey); v != "" {
		return v
	}
	return viper.GetString(envKey)
}

// resolveLogBool mirrors resolveLogString for boolean values: the explicit
// flag/env wins, then the YAML key, then the flag default.
func resolveLogBool(envKey, yamlKey string) bool {
	if viper.IsSet(envKey) {
		return viper.GetBool(envKey)
	}
	if viper.IsSet(yamlKey) {
		return viper.GetBool(yamlKey)
	}
	return viper.GetBool(envKey)
}

func rootFlagBinding(cmd *cobra.Command) {
	viper.BindPFlag("config", cmd.PersistentFlags().Lookup("config"))
	viper.BindPFlag("PGSTREAM_LOG_LEVEL", cmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag("PGSTREAM_LOG_FORMAT", cmd.PersistentFlags().Lookup("log-format"))
	viper.BindPFlag("PGSTREAM_LOG_NO_COLOR", cmd.PersistentFlags().Lookup("no-color"))
}

func version() string {
	if Env != "" {
		return Env + " (" + Version + ")"
	}
	return Version
}

func newInstrumentationProvider() (otel.InstrumentationProvider, error) {
	cfg, err := config.ParseInstrumentationConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing instrumentation config: %w", err)
	}

	p, err := otel.NewInstrumentationProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("initialisating instrumentation provider: %w", err)
	}
	return p, nil
}
