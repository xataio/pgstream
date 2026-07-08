// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"

	"github.com/spf13/viper"
	"github.com/xataio/pgstream/internal/health"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/otel"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/tls"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func init() {
	viper.BindEnv("PGSTREAM_METRICS_ENDPOINT")
	viper.BindEnv("PGSTREAM_METRICS_COLLECTION_INTERVAL")
	viper.BindEnv("PGSTREAM_TRACES_ENDPOINT")
	viper.BindEnv("PGSTREAM_TRACES_SAMPLE_RATIO")

	viper.BindEnv("PGSTREAM_HEALTH_CHECK_ENABLED")
	viper.BindEnv("PGSTREAM_HEALTH_CHECK_ADDRESS")

	viper.BindEnv("PGSTREAM_POSTGRES_LISTENER_URL")
	viper.BindEnv("PGSTREAM_POSTGRES_LISTENER_EXP_BACKOFF_INITIAL_INTERVAL")
	viper.BindEnv("PGSTREAM_POSTGRES_LISTENER_EXP_BACKOFF_MAX_INTERVAL")
	viper.BindEnv("PGSTREAM_POSTGRES_LISTENER_EXP_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_POSTGRES_LISTENER_BACKOFF_INTERVAL")
	viper.BindEnv("PGSTREAM_POSTGRES_LISTENER_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_POSTGRES_LISTENER_DISABLE_RETRIES")
	viper.BindEnv("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME")
	viper.BindEnv("PGSTREAM_POSTGRES_REPLICATION_PLUGIN_INCLUDE_XIDS")

	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_BATCH_BYTES")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_TABLES")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_TABLES")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_WORKERS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_MAX_CONNECTIONS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_STORE_REPEATABLE")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_MODE")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_INCLUDE_GLOBAL_DB_OBJECTS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_ROLE")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_ROLES_SNAPSHOT_MODE")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_CREATE_TARGET_DB")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_DUMP_FILE")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_NO_OWNER")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_NO_PRIVILEGES")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_SECURITY_LABELS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_REFRESH_MATERIALIZED_VIEWS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_DISABLE_PROGRESS_TRACKING")

	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_TARGET_URL")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_SIZE")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_IGNORE_SEND_ERRORS")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_ENABLE")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_MIN_BYTES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_MAX_BYTES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_CONVERGENCE_THRESHOLD")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_EXP_BACKOFF_INITIAL_INTERVAL")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_EXP_BACKOFF_MAX_INTERVAL")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_EXP_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BACKOFF_INTERVAL")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_DISABLE_RETRIES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_IGNORE_DDL")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_STRICT_MODE")

	viper.BindEnv("PGSTREAM_KAFKA_READER_SERVERS")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_SERVERS")
	viper.BindEnv("PGSTREAM_KAFKA_TOPIC_NAME")
	viper.BindEnv("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_ID")
	viper.BindEnv("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_START_OFFSET")
	viper.BindEnv("PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_INITIAL_INTERVAL")
	viper.BindEnv("PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_MAX_INTERVAL")
	viper.BindEnv("PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_KAFKA_COMMIT_BACKOFF_INTERVAL")
	viper.BindEnv("PGSTREAM_KAFKA_COMMIT_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_KAFKA_COMMIT_DISABLE_RETRIES")
	viper.BindEnv("PGSTREAM_KAFKA_TOPIC_PARTITIONS")
	viper.BindEnv("PGSTREAM_KAFKA_TOPIC_REPLICATION_FACTOR")
	viper.BindEnv("PGSTREAM_KAFKA_TOPIC_AUTO_CREATE")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_BATCH_BYTES")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_BATCH_SIZE")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_BATCH_IGNORE_SEND_ERRORS")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_MAX_QUEUE_BYTES")

	viper.BindEnv("PGSTREAM_OPENSEARCH_STORE_URL")
	viper.BindEnv("PGSTREAM_ELASTICSEARCH_STORE_URL")
	viper.BindEnv("PGSTREAM_SEARCH_TLS_ENABLED")
	viper.BindEnv("PGSTREAM_SEARCH_TLS_CA_CERT_FILE")
	viper.BindEnv("PGSTREAM_SEARCH_TLS_CLIENT_CERT_FILE")
	viper.BindEnv("PGSTREAM_SEARCH_TLS_CLIENT_KEY_FILE")
	viper.BindEnv("PGSTREAM_SEARCH_TLS_INSECURE_SKIP_VERIFY")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_BATCH_SIZE")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_MAX_QUEUE_BYTES")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_BATCH_BYTES")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_BATCH_IGNORE_SEND_ERRORS")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_INITIAL_INTERVAL")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_INTERVAL")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_BACKOFF_INTERVAL")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_DISABLE_RETRIES")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_HASH_DOC_IDS")

	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_URL")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_ENABLED")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_REFRESH_INTERVAL")
	viper.BindEnv("PGSTREAM_WEBHOOK_NOTIFIER_MAX_QUEUE_BYTES")
	viper.BindEnv("PGSTREAM_WEBHOOK_NOTIFIER_WORKER_COUNT")
	viper.BindEnv("PGSTREAM_WEBHOOK_NOTIFIER_CLIENT_TIMEOUT")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_ADDRESS")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_READ_TIMEOUT")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_WRITE_TIMEOUT")

	viper.BindEnv("PGSTREAM_STDOUT_WRITER_ENABLED")

	viper.BindEnv("PGSTREAM_INJECTOR_STORE_POSTGRES_URL")
	viper.BindEnv("PGSTREAM_TRANSFORMER_RULES_FILE")
	viper.BindEnv("PGSTREAM_FILTER_INCLUDE_TABLES")
	viper.BindEnv("PGSTREAM_FILTER_EXCLUDE_TABLES")
	viper.BindEnv("PGSTREAM_PROCESSOR_SANITIZE_STRIP_NULL_CHAR_BYTES")

	viper.BindEnv("PGSTREAM_KAFKA_TLS_ENABLED")
	viper.BindEnv("PGSTREAM_KAFKA_TLS_CA_CERT_FILE")
	viper.BindEnv("PGSTREAM_KAFKA_TLS_CLIENT_CERT_FILE")
	viper.BindEnv("PGSTREAM_KAFKA_TLS_CLIENT_KEY_FILE")
}

func envToHealthConfig() *health.Config {
	return &health.Config{
		Enabled: viper.GetBool("PGSTREAM_HEALTH_CHECK_ENABLED"),
		Address: viper.GetString("PGSTREAM_HEALTH_CHECK_ADDRESS"),
	}
}

func envToOtelConfig() (*otel.Config, error) {
	cfg := &otel.Config{}

	metricsEndpoint := viper.GetString("PGSTREAM_METRICS_ENDPOINT")
	if metricsEndpoint != "" {
		cfg.Metrics = &otel.MetricsConfig{
			Endpoint:           metricsEndpoint,
			CollectionInterval: viper.GetDuration("PGSTREAM_METRICS_COLLECTION_INTERVAL"),
		}
	}

	tracesEndpoint := viper.GetString("PGSTREAM_TRACES_ENDPOINT")
	if tracesEndpoint != "" {
		sampleRatio := viper.GetFloat64("PGSTREAM_TRACES_SAMPLE_RATIO")
		if sampleRatio < 0 || sampleRatio > 1 {
			return nil, errInvalidSampleRatio
		}

		cfg.Traces = &otel.TracesConfig{
			Endpoint:    tracesEndpoint,
			SampleRatio: sampleRatio,
		}
	}

	return cfg, nil
}

func envConfigToStreamConfig() (*stream.Config, error) {
	processorCfg, err := parseProcessorConfig()
	if err != nil {
		return nil, err
	}

	listenerCfg, err := parseListenerConfig()
	if err != nil {
		return nil, err
	}

	return &stream.Config{
		Listener:  listenerCfg,
		Processor: processorCfg,
	}, nil
}

// listener parsing

func parseListenerConfig() (stream.ListenerConfig, error) {
	postgresConfig, err := parsePostgresListenerConfig()
	if err != nil {
		return stream.ListenerConfig{}, err
	}

	return stream.ListenerConfig{
		Postgres: postgresConfig,
		Kafka:    parseKafkaListenerConfig(),
	}, nil
}

func parsePostgresListenerConfig() (*stream.PostgresListenerConfig, error) {
	pgURL := viper.GetString("PGSTREAM_POSTGRES_LISTENER_URL")
	if pgURL == "" {
		return nil, nil
	}

	cfg := &stream.PostgresListenerConfig{
		URL: pgURL,
		Replication: pgreplication.Config{
			PostgresURL:         pgURL,
			ReplicationSlotName: viper.GetString("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME"),
			PluginArguments: pgreplication.PluginArguments{
				IncludeXIDs:  viper.GetBool("PGSTREAM_POSTGRES_REPLICATION_PLUGIN_INCLUDE_XIDS"),
				AddTables:    viper.GetString("PGSTREAM_POSTGRES_REPLICATION_PLUGIN_ADD_TABLES"),
				FilterTables: viper.GetString("PGSTREAM_POSTGRES_REPLICATION_PLUGIN_FILTER_TABLES"),
			},
		},
		RetryPolicy: parseBackoffConfig("PGSTREAM_POSTGRES_LISTENER"),
	}

	if filterConfig := parseFilterConfig(); filterConfig != nil {
		cfg.Replication.ExcludeTables = filterConfig.ExcludeTables
		cfg.Replication.IncludeTables = filterConfig.IncludeTables
	}

	snapshotTables := viper.GetStringSlice("PGSTREAM_POSTGRES_SNAPSHOT_TABLES")
	excludedTables := viper.GetStringSlice("PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_TABLES")
	if len(snapshotTables) > 0 || len(excludedTables) > 0 {
		var err error
		cfg.Snapshot, err = parseSnapshotConfig(pgURL)
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func parseSnapshotConfig(pgURL string) (*snapshotbuilder.SnapshotListenerConfig, error) {
	snapshotMode := viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_MODE")

	switch snapshotMode {
	case fullSnapshotMode, dataSnapshotMode, schemaSnapshotMode:
	case "":
		snapshotMode = fullSnapshotMode
	default:
		return nil, errUnsupportedSnapshotMode
	}

	var schemaSnapshotCfg *snapshotbuilder.SchemaSnapshotConfig
	if snapshotMode == fullSnapshotMode || snapshotMode == schemaSnapshotMode {
		var err error
		schemaSnapshotCfg, err = parseSchemaSnapshotConfig(pgURL)
		if err != nil {
			return nil, err
		}
		if schemaSnapshotCfg == nil {
			return nil, errSchemaSnapshotNotConfigured
		}
	}

	var dataSnapshotCfg *pgsnapshotgenerator.Config
	if snapshotMode == fullSnapshotMode || snapshotMode == dataSnapshotMode {
		batchBytes, err := getByteSize("PGSTREAM_POSTGRES_SNAPSHOT_BATCH_BYTES")
		if err != nil {
			return nil, err
		}
		dataSnapshotCfg = &pgsnapshotgenerator.Config{
			URL:             pgURL,
			BatchBytes:      uint64(batchBytes),
			SchemaWorkers:   viper.GetUint("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS"),
			TableWorkers:    viper.GetUint("PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS"),
			SnapshotWorkers: viper.GetUint("PGSTREAM_POSTGRES_SNAPSHOT_WORKERS"),
			MaxConnections:  viper.GetUint("PGSTREAM_POSTGRES_SNAPSHOT_MAX_CONNECTIONS"),
		}
	}

	cfg := &snapshotbuilder.SnapshotListenerConfig{
		Data:   dataSnapshotCfg,
		Schema: schemaSnapshotCfg,
		Adapter: adapter.SnapshotConfig{
			Tables:         viper.GetStringSlice("PGSTREAM_POSTGRES_SNAPSHOT_TABLES"),
			ExcludedTables: viper.GetStringSlice("PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_TABLES"),
		},
		DisableProgressTracking: viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_DISABLE_PROGRESS_TRACKING"),
	}

	if storeURL := viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL"); storeURL != "" {
		cfg.Recorder = &snapshotbuilder.SnapshotRecorderConfig{
			RepeatableSnapshots: viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_STORE_REPEATABLE"),
			SnapshotStoreURL:    storeURL,
			SnapshotWorkers:     viper.GetUint("PGSTREAM_POSTGRES_SNAPSHOT_WORKERS"),
		}
	}

	return cfg, nil
}

func parseSchemaSnapshotConfig(pgurl string) (*snapshotbuilder.SchemaSnapshotConfig, error) {
	pgTargetURL := viper.GetString("PGSTREAM_POSTGRES_WRITER_TARGET_URL")

	rolesSnapshotConfig, err := getRolesSnapshotMode(viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_ROLES_SNAPSHOT_MODE"))
	if err != nil {
		return nil, err
	}
	return &snapshotbuilder.SchemaSnapshotConfig{
		DumpRestore: &pgdumprestore.Config{
			SourcePGURL:              pgurl,
			TargetPGURL:              pgTargetURL,
			CleanTargetDB:            viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB"),
			CreateTargetDB:           viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_CREATE_TARGET_DB"),
			IncludeGlobalDBObjects:   viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_INCLUDE_GLOBAL_DB_OBJECTS"),
			Role:                     viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_ROLE"),
			RolesSnapshotMode:        rolesSnapshotConfig,
			DumpDebugFile:            viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_DUMP_FILE"),
			NoOwner:                  viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_NO_OWNER"),
			NoPrivileges:             viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_NO_PRIVILEGES"),
			ExcludedSecurityLabels:   viper.GetStringSlice("PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_SECURITY_LABELS"),
			RefreshMaterializedViews: viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_REFRESH_MATERIALIZED_VIEWS"),
		},
	}, nil
}

func parseKafkaListenerConfig() *stream.KafkaListenerConfig {
	kafkaTopic := viper.GetString("PGSTREAM_KAFKA_TOPIC_NAME")
	kafkaServers := viper.GetStringSlice("PGSTREAM_KAFKA_READER_SERVERS")
	if len(kafkaServers) == 0 || kafkaTopic == "" {
		return nil
	}

	consumerGroupID := viper.GetString("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_ID")
	return &stream.KafkaListenerConfig{
		Reader:       parseKafkaReaderConfig(kafkaServers, kafkaTopic, consumerGroupID),
		Checkpointer: parseKafkaCheckpointConfig(),
	}
}

func parseKafkaReaderConfig(kafkaServers []string, kafkaTopic, consumerGroupID string) kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Conn: kafka.ConnConfig{
			Servers: kafkaServers,
			Topic: kafka.TopicConfig{
				Name: kafkaTopic,
			},
			TLS: parseTLSConfig("PGSTREAM_KAFKA"),
		},
		ConsumerGroupID:          consumerGroupID,
		ConsumerGroupStartOffset: viper.GetString("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_START_OFFSET"),
	}
}

func parseKafkaCheckpointConfig() kafkacheckpoint.Config {
	return kafkacheckpoint.Config{
		CommitBackoff: parseBackoffConfig("PGSTREAM_KAFKA_COMMIT"),
	}
}

// processor parsing

func parseProcessorConfig() (stream.ProcessorConfig, error) {
	transformerCfg, err := parseTransformerConfig()
	if err != nil {
		return stream.ProcessorConfig{}, err
	}

	kafkaCfg, err := parseKafkaProcessorConfig()
	if err != nil {
		return stream.ProcessorConfig{}, err
	}

	searchCfg, err := parseSearchProcessorConfig()
	if err != nil {
		return stream.ProcessorConfig{}, err
	}

	webhookCfg, err := parseWebhookProcessorConfig()
	if err != nil {
		return stream.ProcessorConfig{}, err
	}

	postgresCfg, err := parsePostgresProcessorConfig()
	if err != nil {
		return stream.ProcessorConfig{}, err
	}

	return stream.ProcessorConfig{
		Kafka:       kafkaCfg,
		Search:      searchCfg,
		Webhook:     webhookCfg,
		Postgres:    postgresCfg,
		Stdout:      parseStdoutProcessorConfig(),
		Injector:    parseInjectorConfig(),
		Transformer: transformerCfg,
		Filter:      parseFilterConfig(),
		Sanitize:    parseSanitizeConfig(),
	}, nil
}

func parseStdoutProcessorConfig() *stream.StdoutProcessorConfig {
	if !viper.GetBool("PGSTREAM_STDOUT_WRITER_ENABLED") {
		return nil
	}
	return &stream.StdoutProcessorConfig{}
}

func parseKafkaProcessorConfig() (*stream.KafkaProcessorConfig, error) {
	kafkaTopic := viper.GetString("PGSTREAM_KAFKA_TOPIC_NAME")
	kafkaServers := viper.GetStringSlice("PGSTREAM_KAFKA_WRITER_SERVERS")
	if len(kafkaServers) == 0 || kafkaTopic == "" {
		return nil, nil
	}

	writerCfg, err := parseKafkaWriterConfig(kafkaServers, kafkaTopic)
	if err != nil {
		return nil, err
	}
	return &stream.KafkaProcessorConfig{
		Writer: writerCfg,
	}, nil
}

func parseKafkaWriterConfig(kafkaServers []string, kafkaTopic string) (*kafkaprocessor.Config, error) {
	maxBatchBytes, err := getByteSize("PGSTREAM_KAFKA_WRITER_BATCH_BYTES")
	if err != nil {
		return nil, err
	}
	maxQueueBytes, err := getByteSize("PGSTREAM_KAFKA_WRITER_MAX_QUEUE_BYTES")
	if err != nil {
		return nil, err
	}
	return &kafkaprocessor.Config{
		Kafka: kafka.ConnConfig{
			Servers: kafkaServers,
			Topic: kafka.TopicConfig{
				Name:              kafkaTopic,
				NumPartitions:     viper.GetInt("PGSTREAM_KAFKA_TOPIC_PARTITIONS"),
				ReplicationFactor: viper.GetInt("PGSTREAM_KAFKA_TOPIC_REPLICATION_FACTOR"),
				AutoCreate:        viper.GetBool("PGSTREAM_KAFKA_TOPIC_AUTO_CREATE"),
			},
			TLS: parseTLSConfig("PGSTREAM_KAFKA"),
		},
		Batch: batch.Config{
			BatchTimeout:     viper.GetDuration("PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT"),
			MaxBatchBytes:    maxBatchBytes,
			MaxBatchSize:     viper.GetInt64("PGSTREAM_KAFKA_WRITER_BATCH_SIZE"),
			MaxQueueBytes:    maxQueueBytes,
			IgnoreSendErrors: viper.GetBool("PGSTREAM_KAFKA_WRITER_BATCH_IGNORE_SEND_ERRORS"),
		},
	}, nil
}

func parseSearchProcessorConfig() (*stream.SearchProcessorConfig, error) {
	opensearchStore := viper.GetString("PGSTREAM_OPENSEARCH_STORE_URL")
	elasticsearchStore := viper.GetString("PGSTREAM_ELASTICSEARCH_STORE_URL")
	if opensearchStore == "" && elasticsearchStore == "" {
		return nil, nil
	}

	maxQueueBytes, err := getByteSize("PGSTREAM_SEARCH_INDEXER_MAX_QUEUE_BYTES")
	if err != nil {
		return nil, err
	}
	maxBatchBytes, err := getByteSize("PGSTREAM_SEARCH_INDEXER_BATCH_BYTES")
	if err != nil {
		return nil, err
	}
	return &stream.SearchProcessorConfig{
		Indexer: search.IndexerConfig{
			Batch: batch.Config{
				MaxBatchSize:     viper.GetInt64("PGSTREAM_SEARCH_INDEXER_BATCH_SIZE"),
				BatchTimeout:     viper.GetDuration("PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT"),
				MaxQueueBytes:    maxQueueBytes,
				MaxBatchBytes:    maxBatchBytes,
				IgnoreSendErrors: viper.GetBool("PGSTREAM_SEARCH_INDEXER_BATCH_IGNORE_SEND_ERRORS"),
			},
			HashDocIDs: viper.GetBool("PGSTREAM_SEARCH_INDEXER_HASH_DOC_IDS"),
		},
		Store: store.Config{
			OpenSearchURL:    opensearchStore,
			ElasticsearchURL: elasticsearchStore,
			TLS:              parseSearchTLSConfig(),
		},
		Retrier: search.StoreRetryConfig{
			Backoff: parseBackoffConfig("PGSTREAM_SEARCH_STORE"),
		},
	}, nil
}

func parseWebhookProcessorConfig() (*stream.WebhookProcessorConfig, error) {
	subscriptionStore := viper.GetString("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_URL")
	if subscriptionStore == "" {
		return nil, nil
	}

	maxQueueBytes, err := getByteSize("PGSTREAM_WEBHOOK_NOTIFIER_MAX_QUEUE_BYTES")
	if err != nil {
		return nil, err
	}
	return &stream.WebhookProcessorConfig{
		SubscriptionStore: stream.WebhookSubscriptionStoreConfig{
			URL:                  subscriptionStore,
			CacheEnabled:         viper.GetBool("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_ENABLED"),
			CacheRefreshInterval: viper.GetDuration("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_REFRESH_INTERVAL"),
		},
		Notifier: notifier.Config{
			MaxQueueBytes:  maxQueueBytes,
			URLWorkerCount: viper.GetUint("PGSTREAM_WEBHOOK_NOTIFIER_WORKER_COUNT"),
			ClientTimeout:  viper.GetDuration("PGSTREAM_WEBHOOK_NOTIFIER_CLIENT_TIMEOUT"),
		},
		SubscriptionServer: server.Config{
			Address:      viper.GetString("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_ADDRESS"),
			ReadTimeout:  viper.GetDuration("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_READ_TIMEOUT"),
			WriteTimeout: viper.GetDuration("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_WRITE_TIMEOUT"),
		},
	}, nil
}

func parsePostgresProcessorConfig() (*stream.PostgresProcessorConfig, error) {
	targetPostgresURL := viper.GetString("PGSTREAM_POSTGRES_WRITER_TARGET_URL")
	if targetPostgresURL == "" {
		return nil, nil
	}

	maxBatchBytes, err := getByteSize("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES")
	if err != nil {
		return nil, err
	}
	maxQueueBytes, err := getByteSize("PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES")
	if err != nil {
		return nil, err
	}
	autoTuneMinBytes, err := getByteSize("PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_MIN_BYTES")
	if err != nil {
		return nil, err
	}
	autoTuneMaxBytes, err := getByteSize("PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_MAX_BYTES")
	if err != nil {
		return nil, err
	}

	bulkIngestEnabled := viper.GetBool("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED")
	cfg := &stream.PostgresProcessorConfig{
		BatchWriter: postgres.Config{
			URL: targetPostgresURL,
			BatchConfig: batch.Config{
				BatchTimeout:     viper.GetDuration("PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT"),
				MaxBatchBytes:    maxBatchBytes,
				MaxBatchSize:     viper.GetInt64("PGSTREAM_POSTGRES_WRITER_BATCH_SIZE"),
				MaxQueueBytes:    maxQueueBytes,
				IgnoreSendErrors: viper.GetBool("PGSTREAM_POSTGRES_WRITER_BATCH_IGNORE_SEND_ERRORS"),
				AutoTune: batch.AutoTuneConfig{
					Enabled:              viper.GetBool("PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_ENABLE"),
					MinBatchBytes:        autoTuneMinBytes,
					MaxBatchBytes:        autoTuneMaxBytes,
					ConvergenceThreshold: viper.GetFloat64("PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_CONVERGENCE_THRESHOLD"),
				},
			},
			DisableTriggers:   viper.GetBool("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS"),
			OnConflictAction:  viper.GetString("PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION"),
			BulkIngestEnabled: bulkIngestEnabled,
			RetryPolicy:       parseBackoffConfig("PGSTREAM_POSTGRES_WRITER"),
			IgnoreDDL:         viper.GetBool("PGSTREAM_POSTGRES_WRITER_IGNORE_DDL"),
			StrictMode:        viper.GetBool("PGSTREAM_POSTGRES_WRITER_STRICT_MODE"),
		},
	}

	if bulkIngestEnabled {
		applyPostgresBulkBatchDefaults(&cfg.BatchWriter.BatchConfig)
	}

	return cfg, nil
}

func parseBackoffConfig(prefix string) backoff.Config {
	return backoff.Config{
		DisableRetries: viper.GetBool(fmt.Sprintf("%s_DISABLE_RETRIES", prefix)),
		Exponential:    parseExponentialBackoffConfig(prefix),
		Constant:       parseConstantBackoffConfig(prefix),
	}
}

func parseExponentialBackoffConfig(prefix string) *backoff.ExponentialConfig {
	initialInterval := viper.GetDuration(fmt.Sprintf("%s_EXP_BACKOFF_INITIAL_INTERVAL", prefix))
	maxInterval := viper.GetDuration(fmt.Sprintf("%s_EXP_BACKOFF_MAX_INTERVAL", prefix))
	maxElapsedTime := viper.GetDuration(fmt.Sprintf("%s_EXP_BACKOFF_MAX_ELAPSED_TIME", prefix))
	maxRetries := viper.GetUint(fmt.Sprintf("%s_EXP_BACKOFF_MAX_RETRIES", prefix))
	if initialInterval == 0 && maxInterval == 0 && maxElapsedTime == 0 && maxRetries == 0 {
		return nil
	}
	return &backoff.ExponentialConfig{
		InitialInterval: initialInterval,
		MaxInterval:     maxInterval,
		MaxElapsedTime:  maxElapsedTime,
		MaxRetries:      maxRetries,
	}
}

func parseConstantBackoffConfig(prefix string) *backoff.ConstantConfig {
	interval := viper.GetDuration(fmt.Sprintf("%s_BACKOFF_INTERVAL", prefix))
	maxRetries := viper.GetUint(fmt.Sprintf("%s_BACKOFF_MAX_RETRIES", prefix))
	if interval == 0 && maxRetries == 0 {
		return nil
	}
	return &backoff.ConstantConfig{
		Interval:   interval,
		MaxRetries: maxRetries,
	}
}

func parseInjectorConfig() *injector.Config {
	pgURL := viper.GetString("PGSTREAM_INJECTOR_STORE_POSTGRES_URL")
	if pgURL == "" {
		return nil
	}
	return &injector.Config{
		URL: pgURL,
	}
}

func parseTransformerConfig() (*transformer.Config, error) {
	filename := viper.GetString("PGSTREAM_TRANSFORMER_RULES_FILE")
	return ParseTransformerConfig(filename)
}

func parseFilterConfig() *filter.Config {
	includeTables := viper.GetStringSlice("PGSTREAM_FILTER_INCLUDE_TABLES")
	excludeTables := viper.GetStringSlice("PGSTREAM_FILTER_EXCLUDE_TABLES")
	if len(includeTables) == 0 && len(excludeTables) == 0 {
		return nil
	}

	return &filter.Config{
		IncludeTables: includeTables,
		ExcludeTables: excludeTables,
	}
}

func parseSanitizeConfig() *stream.SanitizeConfig {
	if !viper.GetBool("PGSTREAM_PROCESSOR_SANITIZE_STRIP_NULL_CHAR_BYTES") {
		return nil
	}
	return &stream.SanitizeConfig{
		StripNullCharBytes: true,
	}
}

func parseTLSConfig(prefix string) tls.Config {
	return tls.Config{
		Enabled:        viper.GetBool(fmt.Sprintf("%s_TLS_ENABLED", prefix)),
		CaCertFile:     viper.GetString(fmt.Sprintf("%s_TLS_CA_CERT_FILE", prefix)),
		ClientCertFile: viper.GetString(fmt.Sprintf("%s_TLS_CLIENT_CERT_FILE", prefix)),
		ClientKeyFile:  viper.GetString(fmt.Sprintf("%s_TLS_CLIENT_KEY_FILE", prefix)),
	}
}

// parseSearchTLSConfig reads the search store TLS settings from the
// environment.
func parseSearchTLSConfig() tls.Config {
	cfg := tls.Config{
		CaCertFile:         viper.GetString("PGSTREAM_SEARCH_TLS_CA_CERT_FILE"),
		ClientCertFile:     viper.GetString("PGSTREAM_SEARCH_TLS_CLIENT_CERT_FILE"),
		ClientKeyFile:      viper.GetString("PGSTREAM_SEARCH_TLS_CLIENT_KEY_FILE"),
		InsecureSkipVerify: viper.GetBool("PGSTREAM_SEARCH_TLS_INSECURE_SKIP_VERIFY"),
	}
	cfg.Enabled = viper.GetBool("PGSTREAM_SEARCH_TLS_ENABLED") ||
		cfg.CaCertFile != "" || cfg.ClientCertFile != "" || cfg.InsecureSkipVerify
	return cfg
}
