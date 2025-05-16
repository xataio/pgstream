// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/internal/log/zerolog"
	"github.com/xataio/pgstream/pkg/stream"
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "Run starts a continuous data stream from the configured source to the configured target",
	PreRunE: runFlagBinding,
	RunE:    withProfiling(withSignalWatcher(run)),
	Example: `
	pgstream run --source postgres --source-url <source-postgres-url> --target postgres --target-url <target-postgres-url>
	pgstream run --source postgres --source-url <source-postgres-url> --target postgres --target-url <target-postgres-url> --snapshot-tables <schema.table> --reset
	pgstream run --source kafka --source-url <kafka-url> --target elasticsearch --target-url <elasticsearch-url>
	pgstream run --source postgres --source-url <postgres-url> --target kafka --target-url <kafka-url>
	pgstream run --config config.yaml --log-level info
	pgstream run --config config.env`,
}

var (
	errUnsupportedSource = errors.New("unsupported source")
	errUnsupportedTarget = errors.New("unsupported target")
)

const (
	defaultKafkaTopicName = "pgstream"
	postgres              = "postgres"
)

func run(ctx context.Context) error {
	logger := zerolog.NewLogger(&zerolog.Config{
		LogLevel: viper.GetString("PGSTREAM_LOG_LEVEL"),
	})
	zerolog.SetGlobalLogger(logger)

	streamConfig, err := config.ParseStreamConfig()
	if err != nil {
		return fmt.Errorf("parsing stream config: %w", err)
	}

	provider, err := newInstrumentationProvider()
	if err != nil {
		return err
	}
	defer provider.Close()

	return stream.Run(ctx, zerolog.NewStdLogger(logger), streamConfig, provider.NewInstrumentation("run"))
}

func runFlagBinding(cmd *cobra.Command, args []string) error {
	if err := sourceFlagBinding(cmd); err != nil {
		return fmt.Errorf("failed to bind source flags: %w", err)
	}

	if err := targetFlagBinding(cmd); err != nil {
		return fmt.Errorf("failed to bind target flags: %w", err)
	}

	initialSnapshotFlagBinding(cmd)

	return nil
}

func initialSnapshotFlagBinding(cmd *cobra.Command) {
	// to be able to overwrite configuration with flags when yaml config file is
	// provided
	if cmd.Flags().Lookup("snapshot-tables").Changed {
		viper.BindPFlag("source.postgres.snapshot.tables", cmd.Flags().Lookup("snapshot-tables"))
		viper.BindPFlag("source.postgres.snapshot.schema.pgdump_pgrestore.clean_target_db", cmd.Flags().Lookup("reset"))
		if len(viper.GetStringSlice("source.postgres.snapshot.tables")) > 0 {
			viper.Set("source.postgres.mode", "snapshot_and_replication")
			viper.Set("source.postgres.snapshot.mode", "full")
			viper.Set("source.postgres.snapshot.schema.mode", "schemalog")
			if cmd.Flags().Lookup("target").Value.String() == postgres {
				viper.Set("source.postgres.snapshot.schema.mode", "pgdump_pgrestore")
			}
		}
	}

	// to be able to overwrite configuration with flags when env config file is
	// provided or when no configuration is provided
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_TABLES", cmd.Flags().Lookup("snapshot-tables"))
	viper.BindPFlag("PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB", cmd.Flags().Lookup("reset"))
}

func sourceFlagBinding(cmd *cobra.Command) error {
	// source flag is not set
	if !cmd.Flags().Lookup("source").Changed {
		return nil
	}
	switch source := cmd.Flags().Lookup("source").Value.String(); source {
	case postgres:
		viper.BindPFlag("source.postgres.url", cmd.Flags().Lookup("source-url"))
		viper.BindPFlag("PGSTREAM_POSTGRES_LISTENER_URL", cmd.Flags().Lookup("source-url"))
		// enable metadata injection by default when using postgres as source
		viper.Set("modifiers.injector.enabled", true)
		viper.BindPFlag("PGSTREAM_INJECTOR_STORE_POSTGRES_URL", cmd.Flags().Lookup("source-url"))
	case "kafka":
		viper.BindPFlag("source.kafka.servers", cmd.Flags().Lookup("source-url"))
		viper.Set("target.kafka.topic.name", defaultKafkaTopicName)

		viper.BindPFlag("PGSTREAM_KAFKA_READER_SERVERS", cmd.Flags().Lookup("source-url"))
		viper.Set("PGSTREAM_KAFKA_TOPIC_NAME", defaultKafkaTopicName)
	default:
		return errUnsupportedSource
	}

	return nil
}

func targetFlagBinding(cmd *cobra.Command) error {
	// target flag is not set
	if !cmd.Flags().Lookup("target").Changed {
		return nil
	}
	switch target := cmd.Flags().Lookup("target").Value.String(); target {
	case postgres:
		viper.BindPFlag("target.postgres.url", cmd.Flags().Lookup("target-url"))
		viper.BindPFlag("PGSTREAM_POSTGRES_WRITER_TARGET_URL", cmd.Flags().Lookup("target-url"))
		// disable triggers by default when running on snapshot mode
		if cmd.Name() == "snapshot" {
			if viper.GetString("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS") == "" {
				viper.Set("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS", true)
			}
			if viper.GetString("target.postgres.disable_triggers") == "" {
				viper.Set("target.postgres.disable_triggers", true)
			}
		}
	case "elasticsearch", "opensearch":
		viper.Set("target.search.engine", target)
		viper.BindPFlag("target.search.url", cmd.Flags().Lookup("target-url"))
		if target == "elasticsearch" {
			viper.BindPFlag("PGSTREAM_ELASTICSEARCH_STORE_URL", cmd.Flags().Lookup("target-url"))
		}
		if target == "opensearch" {
			viper.BindPFlag("PGSTREAM_OPENSEARCH_STORE_URL", cmd.Flags().Lookup("target-url"))
		}
	case "kafka":
		viper.BindPFlag("target.kafka.servers", cmd.Flags().Lookup("target-url"))
		viper.Set("target.kafka.topic.name", defaultKafkaTopicName)
		viper.Set("target.kafka.topic.auto_create", true)

		viper.BindPFlag("PGSTREAM_KAFKA_WRITER_SERVERS", cmd.Flags().Lookup("target-url"))
		viper.Set("PGSTREAM_KAFKA_TOPIC_NAME", defaultKafkaTopicName)
		viper.Set("PGSTREAM_KAFKA_TOPIC_AUTO_CREATE", true)
	default:
		return errUnsupportedTarget
	}

	return nil
}
