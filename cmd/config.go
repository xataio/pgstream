// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/spf13/viper"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	pgschemalog "github.com/xataio/pgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/data/postgres"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/tls"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	pglistener "github.com/xataio/pgstream/pkg/wal/listener/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func loadConfig() error {
	cfgFile := viper.GetString("config")
	if cfgFile != "" {
		fmt.Printf("using config file: %s\n", cfgFile) //nolint:forbidigo //logger hasn't been configured yet
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("error reading config: %w", err)
		}
	}
	return nil
}

func pgURL() string {
	pgurl := viper.GetString("pgurl")
	if pgurl != "" {
		return pgurl
	}
	return viper.GetString("PGSTREAM_POSTGRES_LISTENER_URL")
}

func replicationSlotName() string {
	replicationslot := viper.GetString("replication-slot")
	if replicationslot != "" {
		return replicationslot
	}
	return viper.GetString("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME")
}

func parseStreamConfig() *stream.Config {
	return &stream.Config{
		Listener:  parseListenerConfig(),
		Processor: parseProcessorConfig(),
	}
}

// listener parsing

func parseListenerConfig() stream.ListenerConfig {
	return stream.ListenerConfig{
		Postgres: parsePostgresListenerConfig(),
		Kafka:    parseKafkaListenerConfig(),
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
			ReplicationSlotName: replicationSlotName(),
		},
	}

	snapshotTables := viper.GetStringSlice("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLES")
	if len(snapshotTables) > 0 {
		cfg.Snapshot = &pglistener.SnapshotConfig{
			SchemaLogStore: pgschemalog.Config{
				URL: pgURL,
			},
			SnapshotStoreURL: pgURL,
			Generator: pgsnapshotgenerator.Config{
				URL:           pgURL,
				BatchPageSize: viper.GetUint("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_BATCH_PAGE_SIZE"),
				SchemaWorkers: viper.GetUint("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_SCHEMA_WORKERS"),
				TableWorkers:  viper.GetUint("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLE_WORKERS"),
			},
			Tables:          snapshotTables,
			SnapshotWorkers: viper.GetUint("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_WORKERS"),
		}
	}

	return cfg
}

func parseKafkaListenerConfig() *stream.KafkaListenerConfig {
	kafkaServers := viper.GetStringSlice("PGSTREAM_KAFKA_SERVERS")
	kafkaTopic := viper.GetString("PGSTREAM_KAFKA_TOPIC_NAME")
	consumerGroupID := viper.GetString("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_ID")
	if len(kafkaServers) == 0 || kafkaTopic == "" || consumerGroupID == "" {
		return nil
	}

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

func parseProcessorConfig() stream.ProcessorConfig {
	return stream.ProcessorConfig{
		Kafka:    parseKafkaProcessorConfig(),
		Search:   parseSearchProcessorConfig(),
		Webhook:  parseWebhookProcessorConfig(),
		Injector: parseInjectorConfig(),
	}
}

func parseKafkaProcessorConfig() *stream.KafkaProcessorConfig {
	kafkaServers := viper.GetStringSlice("PGSTREAM_KAFKA_SERVERS")
	kafkaTopic := viper.GetString("PGSTREAM_KAFKA_TOPIC_NAME")
	topicPartitions := viper.GetInt("PGSTREAM_KAFKA_TOPIC_PARTITIONS")
	if len(kafkaServers) == 0 || kafkaTopic == "" || topicPartitions == 0 {
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

func parseTLSConfig(prefix string) tls.Config {
	return tls.Config{
		Enabled:        viper.GetBool(fmt.Sprintf("%s_TLS_ENABLED", prefix)),
		CaCertFile:     viper.GetString(fmt.Sprintf("%s_TLS_CA_CERT_FILE", prefix)),
		ClientCertFile: viper.GetString(fmt.Sprintf("%s_TLS_CLIENT_CERT_FILE", prefix)),
		ClientKeyFile:  viper.GetString(fmt.Sprintf("%s_TLS_CLIENT_KEY_FILE", prefix)),
	}
}
