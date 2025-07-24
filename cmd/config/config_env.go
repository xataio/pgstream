// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/otel"
	pgschemalog "github.com/xataio/pgstream/pkg/schemalog/postgres"
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
	"gopkg.in/yaml.v3"
)

func init() {
	viper.BindEnv("PGSTREAM_METRICS_ENDPOINT")
	viper.BindEnv("PGSTREAM_METRICS_COLLECTION_INTERVAL")
	viper.BindEnv("PGSTREAM_TRACES_ENDPOINT")
	viper.BindEnv("PGSTREAM_TRACES_SAMPLE_RATIO")

	viper.BindEnv("PGSTREAM_POSTGRES_LISTENER_URL")
	viper.BindEnv("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME")

	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_BATCH_BYTES")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_TABLES")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_TABLES")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_WORKERS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_STORE_REPEATABLE")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_USE_SCHEMALOG")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_INCLUDE_GLOBAL_DB_OBJECTS")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_ROLE")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_CREATE_TARGET_DB")
	viper.BindEnv("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_DUMP_FILE")

	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_TARGET_URL")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BATCH_SIZE")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION")
	viper.BindEnv("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED")

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
	viper.BindEnv("PGSTREAM_KAFKA_TOPIC_PARTITIONS")
	viper.BindEnv("PGSTREAM_KAFKA_TOPIC_REPLICATION_FACTOR")
	viper.BindEnv("PGSTREAM_KAFKA_TOPIC_AUTO_CREATE")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_BATCH_BYTES")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_BATCH_SIZE")
	viper.BindEnv("PGSTREAM_KAFKA_WRITER_MAX_QUEUE_BYTES")

	viper.BindEnv("PGSTREAM_OPENSEARCH_STORE_URL")
	viper.BindEnv("PGSTREAM_ELASTICSEARCH_STORE_URL")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_BATCH_SIZE")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_MAX_QUEUE_BYTES")
	viper.BindEnv("PGSTREAM_SEARCH_INDEXER_BATCH_BYTES")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_INITIAL_INTERVAL")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_INTERVAL")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_RETRIES")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_BACKOFF_INTERVAL")
	viper.BindEnv("PGSTREAM_SEARCH_STORE_BACKOFF_MAX_RETRIES")

	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_URL")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_ENABLED")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_REFRESH_INTERVAL")
	viper.BindEnv("PGSTREAM_WEBHOOK_NOTIFIER_MAX_QUEUE_BYTES")
	viper.BindEnv("PGSTREAM_WEBHOOK_NOTIFIER_WORKER_COUNT")
	viper.BindEnv("PGSTREAM_WEBHOOK_NOTIFIER_CLIENT_TIMEOUT")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_ADDRESS")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_READ_TIMEOUT")
	viper.BindEnv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_WRITE_TIMEOUT")

	viper.BindEnv("PGSTREAM_INJECTOR_STORE_POSTGRES_URL")
	viper.BindEnv("PGSTREAM_TRANSFORMER_RULES_FILE")
	viper.BindEnv("PGSTREAM_FILTER_INCLUDE_TABLES")
	viper.BindEnv("PGSTREAM_FILTER_EXCLUDE_TABLES")

	viper.BindEnv("PGSTREAM_KAFKA_TLS_ENABLED")
	viper.BindEnv("PGSTREAM_KAFKA_TLS_CA_CERT_FILE")
	viper.BindEnv("PGSTREAM_KAFKA_TLS_CLIENT_CERT_FILE")
	viper.BindEnv("PGSTREAM_KAFKA_TLS_CLIENT_KEY_FILE")
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
	return &stream.Config{
		Listener:  parseListenerConfig(),
		Processor: processorCfg,
	}, nil
}

// listener parsing

func parseListenerConfig() stream.ListenerConfig {
	return stream.ListenerConfig{
		Postgres: parsePostgresListenerConfig(),
		Kafka:    parseKafkaListenerConfig(),
		Snapshot: parseSnapshotListenerConfig(),
	}
}

func parsePostgresListenerConfig() *stream.PostgresListenerConfig {
	pgURL := viper.GetString("PGSTREAM_POSTGRES_LISTENER_URL")
	if pgURL == "" {
		return nil
	}

	cfg := &stream.PostgresListenerConfig{
		Replication: pgreplication.Config{
			PostgresURL:         pgURL,
			ReplicationSlotName: viper.GetString("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME"),
		},
	}

	snapshotTables := viper.GetStringSlice("PGSTREAM_POSTGRES_SNAPSHOT_TABLES")
	if len(snapshotTables) > 0 {
		cfg.Snapshot = parseSnapshotConfig(pgURL)
	}

	return cfg
}

func parseSnapshotListenerConfig() *snapshotbuilder.SnapshotListenerConfig {
	pgsnapshotURL := viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL")
	if pgsnapshotURL == "" {
		return nil
	}
	return parseSnapshotConfig(pgsnapshotURL)
}

func parseSnapshotConfig(pgURL string) *snapshotbuilder.SnapshotListenerConfig {
	cfg := &snapshotbuilder.SnapshotListenerConfig{
		Generator: pgsnapshotgenerator.Config{
			URL:             pgURL,
			BatchBytes:      viper.GetUint64("PGSTREAM_POSTGRES_SNAPSHOT_BATCH_BYTES"),
			SchemaWorkers:   viper.GetUint("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS"),
			TableWorkers:    viper.GetUint("PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS"),
			SnapshotWorkers: viper.GetUint("PGSTREAM_POSTGRES_SNAPSHOT_WORKERS"),
		},
		Adapter: adapter.SnapshotConfig{
			Tables:         viper.GetStringSlice("PGSTREAM_POSTGRES_SNAPSHOT_TABLES"),
			ExcludedTables: viper.GetStringSlice("PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_TABLES"),
		},
		Schema: parseSchemaSnapshotConfig(pgURL),
	}

	if storeURL := viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL"); storeURL != "" {
		cfg.Recorder = &snapshotbuilder.SnapshotRecorderConfig{
			RepeatableSnapshots: viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_STORE_REPEATABLE"),
			SnapshotStoreURL:    storeURL,
		}
	}

	return cfg
}

func parseSchemaSnapshotConfig(pgurl string) snapshotbuilder.SchemaSnapshotConfig {
	useSchemaLog := viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_USE_SCHEMALOG")
	pgTargetURL := viper.GetString("PGSTREAM_POSTGRES_WRITER_TARGET_URL")
	if pgTargetURL != "" && !useSchemaLog {
		return snapshotbuilder.SchemaSnapshotConfig{
			DumpRestore: &pgdumprestore.Config{
				SourcePGURL:            pgurl,
				TargetPGURL:            pgTargetURL,
				CleanTargetDB:          viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB"),
				CreateTargetDB:         viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_CREATE_TARGET_DB"),
				IncludeGlobalDBObjects: viper.GetBool("PGSTREAM_POSTGRES_SNAPSHOT_INCLUDE_GLOBAL_DB_OBJECTS"),
				Role:                   viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_ROLE"),
				DumpDebugFile:          viper.GetString("PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_DUMP_FILE"),
			},
		}
	}
	return snapshotbuilder.SchemaSnapshotConfig{
		SchemaLogStore: &pgschemalog.Config{
			URL: pgurl,
		},
	}
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
	return stream.ProcessorConfig{
		Kafka:       parseKafkaProcessorConfig(),
		Search:      parseSearchProcessorConfig(),
		Webhook:     parseWebhookProcessorConfig(),
		Postgres:    parsePostgresProcessorConfig(),
		Injector:    parseInjectorConfig(),
		Transformer: transformerCfg,
		Filter:      parseFilterConfig(),
	}, nil
}

func parseKafkaProcessorConfig() *stream.KafkaProcessorConfig {
	kafkaTopic := viper.GetString("PGSTREAM_KAFKA_TOPIC_NAME")
	kafkaServers := viper.GetStringSlice("PGSTREAM_KAFKA_WRITER_SERVERS")
	if len(kafkaServers) == 0 || kafkaTopic == "" {
		return nil
	}

	return &stream.KafkaProcessorConfig{
		Writer: parseKafkaWriterConfig(kafkaServers, kafkaTopic),
	}
}

func parseKafkaWriterConfig(kafkaServers []string, kafkaTopic string) *kafkaprocessor.Config {
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
			BatchTimeout:  viper.GetDuration("PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT"),
			MaxBatchBytes: viper.GetInt64("PGSTREAM_KAFKA_WRITER_BATCH_BYTES"),
			MaxBatchSize:  viper.GetInt64("PGSTREAM_KAFKA_WRITER_BATCH_SIZE"),
			MaxQueueBytes: viper.GetInt64("PGSTREAM_KAFKA_WRITER_MAX_QUEUE_BYTES"),
		},
	}
}

func parseSearchProcessorConfig() *stream.SearchProcessorConfig {
	opensearchStore := viper.GetString("PGSTREAM_OPENSEARCH_STORE_URL")
	elasticsearchStore := viper.GetString("PGSTREAM_ELASTICSEARCH_STORE_URL")
	if opensearchStore == "" && elasticsearchStore == "" {
		return nil
	}

	return &stream.SearchProcessorConfig{
		Indexer: search.IndexerConfig{
			Batch: batch.Config{
				MaxBatchSize:  viper.GetInt64("PGSTREAM_SEARCH_INDEXER_BATCH_SIZE"),
				BatchTimeout:  viper.GetDuration("PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT"),
				MaxQueueBytes: viper.GetInt64("PGSTREAM_SEARCH_INDEXER_MAX_QUEUE_BYTES"),
				MaxBatchBytes: viper.GetInt64("PGSTREAM_SEARCH_INDEXER_BATCH_BYTES"),
			},
		},
		Store: store.Config{
			OpenSearchURL:    opensearchStore,
			ElasticsearchURL: elasticsearchStore,
		},
		Retrier: search.StoreRetryConfig{
			Backoff: parseBackoffConfig("PGSTREAM_SEARCH_STORE"),
		},
	}
}

func parseWebhookProcessorConfig() *stream.WebhookProcessorConfig {
	subscriptionStore := viper.GetString("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_URL")
	if subscriptionStore == "" {
		return nil
	}

	return &stream.WebhookProcessorConfig{
		SubscriptionStore: stream.WebhookSubscriptionStoreConfig{
			URL:                  subscriptionStore,
			CacheEnabled:         viper.GetBool("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_ENABLED"),
			CacheRefreshInterval: viper.GetDuration("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_REFRESH_INTERVAL"),
		},
		Notifier: notifier.Config{
			MaxQueueBytes:  viper.GetInt64("PGSTREAM_WEBHOOK_NOTIFIER_MAX_QUEUE_BYTES"),
			URLWorkerCount: viper.GetUint("PGSTREAM_WEBHOOK_NOTIFIER_WORKER_COUNT"),
			ClientTimeout:  viper.GetDuration("PGSTREAM_WEBHOOK_NOTIFIER_CLIENT_TIMEOUT"),
		},
		SubscriptionServer: server.Config{
			Address:      viper.GetString("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_ADDRESS"),
			ReadTimeout:  viper.GetDuration("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_READ_TIMEOUT"),
			WriteTimeout: viper.GetDuration("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_WRITE_TIMEOUT"),
		},
	}
}

func parsePostgresProcessorConfig() *stream.PostgresProcessorConfig {
	targetPostgresURL := viper.GetString("PGSTREAM_POSTGRES_WRITER_TARGET_URL")
	if targetPostgresURL == "" {
		return nil
	}

	bulkIngestEnabled := viper.GetBool("PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED")
	cfg := &stream.PostgresProcessorConfig{
		BatchWriter: postgres.Config{
			URL: targetPostgresURL,
			BatchConfig: batch.Config{
				BatchTimeout:  viper.GetDuration("PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT"),
				MaxBatchBytes: viper.GetInt64("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES"),
				MaxBatchSize:  viper.GetInt64("PGSTREAM_POSTGRES_WRITER_BATCH_SIZE"),
				MaxQueueBytes: viper.GetInt64("PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES"),
			},
			SchemaLogStore: pgschemalog.Config{
				URL: viper.GetString("PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL"),
			},
			DisableTriggers:   viper.GetBool("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS"),
			OnConflictAction:  viper.GetString("PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION"),
			BulkIngestEnabled: bulkIngestEnabled,
		},
	}

	if bulkIngestEnabled {
		applyPostgresBulkBatchDefaults(&cfg.BatchWriter.BatchConfig)
	}

	return cfg
}

func parseBackoffConfig(prefix string) backoff.Config {
	return backoff.Config{
		Exponential: parseExponentialBackoffConfig(prefix),
		Constant:    parseConstantBackoffConfig(prefix),
	}
}

func parseExponentialBackoffConfig(prefix string) *backoff.ExponentialConfig {
	initialInterval := viper.GetDuration(fmt.Sprintf("%s_EXP_BACKOFF_INITIAL_INTERVAL", prefix))
	maxInterval := viper.GetDuration(fmt.Sprintf("%s_EXP_BACKOFF_MAX_INTERVAL", prefix))
	maxRetries := viper.GetUint(fmt.Sprintf("%s_EXP_BACKOFF_MAX_RETRIES", prefix))
	if initialInterval == 0 && maxInterval == 0 && maxRetries == 0 {
		return nil
	}
	return &backoff.ExponentialConfig{
		InitialInterval: initialInterval,
		MaxInterval:     maxInterval,
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
		Store: pgschemalog.Config{
			URL: pgURL,
		},
	}
}

func parseTransformerConfig() (*transformer.Config, error) {
	filename := viper.GetString("PGSTREAM_TRANSFORMER_RULES_FILE")
	if filename == "" {
		return nil, nil
	}

	buf, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	yamlConfig := struct {
		Transformations TransformationsConfig `mapstructure:"transformations" yaml:"transformations"`
	}{}
	err = yaml.Unmarshal(buf, &yamlConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid format for transformations config in file %q: %w", filename, err)
	}

	return yamlConfig.Transformations.parseTransformationConfig()
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

func parseTLSConfig(prefix string) tls.Config {
	return tls.Config{
		Enabled:        viper.GetBool(fmt.Sprintf("%s_TLS_ENABLED", prefix)),
		CaCertFile:     viper.GetString(fmt.Sprintf("%s_TLS_CA_CERT_FILE", prefix)),
		ClientCertFile: viper.GetString(fmt.Sprintf("%s_TLS_CLIENT_CERT_FILE", prefix)),
		ClientKeyFile:  viper.GetString(fmt.Sprintf("%s_TLS_CLIENT_KEY_FILE", prefix)),
	}
}
