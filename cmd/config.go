// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/spf13/viper"
	"github.com/xataio/pgstream/internal/backoff"
	"github.com/xataio/pgstream/internal/kafka"
	"github.com/xataio/pgstream/internal/tls"
	pgschemalog "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/stream"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/opensearch"
	"github.com/xataio/pgstream/pkg/wal/processor/translator"
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

	return &stream.PostgresListenerConfig{
		Replication: pgreplication.Config{
			PostgresURL: pgURL,
		},
	}
}

func parseKafkaListenerConfig() *stream.KafkaListenerConfig {
	kafkaServers := viper.GetStringSlice("PGSTREAM_KAFKA_SERVERS")
	kafkaTopic := viper.GetString("PGSTREAM_KAFKA_TOPIC_NAME")
	consumerGroupID := viper.GetString("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_ID")
	if len(kafkaServers) == 0 || kafkaTopic == "" || consumerGroupID == "" {
		return nil
	}

	readerCfg := parseKafkaReaderConfig(kafkaServers, kafkaTopic, consumerGroupID)

	return &stream.KafkaListenerConfig{
		Reader:       readerCfg,
		Checkpointer: parseKafkaCheckpointConfig(&readerCfg),
	}
}

func parseKafkaReaderConfig(kafkaServers []string, kafkaTopic, consumerGroupID string) kafkalistener.ReaderConfig {
	return kafkalistener.ReaderConfig{
		Kafka: kafka.ReaderConfig{
			Conn: kafka.ConnConfig{
				Servers: kafkaServers,
				Topic: kafka.TopicConfig{
					Name: kafkaTopic,
				},
				TLS: parseTLSConfig("PGSTREAM_KAFKA"),
			},
			ConsumerGroupID:          consumerGroupID,
			ConsumerGroupStartOffset: viper.GetString("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_START_OFFSET"),
		},
	}
}

func parseKafkaCheckpointConfig(readerCfg *kafkalistener.ReaderConfig) kafkacheckpoint.Config {
	return kafkacheckpoint.Config{
		Reader:        readerCfg.Kafka,
		CommitBackoff: parseBackoffConfig("PGSTREAM_KAFKA_COMMIT"),
	}
}

// processor parsing

func parseProcessorConfig() stream.ProcessorConfig {
	return stream.ProcessorConfig{
		Kafka:      parseKafkaProcessorConfig(),
		Search:     parseSearchProcessorConfig(),
		Webhook:    parseWebhookProcessorConfig(),
		Translator: parseTranslatorConfig(),
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
		BatchTimeout:  viper.GetDuration("PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT"),
		BatchBytes:    viper.GetInt64("PGSTREAM_KAFKA_WRITER_BATCH_BYTES"),
		BatchSize:     viper.GetInt("PGSTREAM_KAFKA_WRITER_BATCH_SIZE"),
		MaxQueueBytes: viper.GetInt64("PGSTREAM_KAFKA_WRITER_MAX_QUEUE_BYTES"),
	}
}

func parseSearchProcessorConfig() *stream.SearchProcessorConfig {
	searchStore := viper.GetString("PGSTREAM_SEARCH_STORE_URL")
	if searchStore == "" {
		return nil
	}

	return &stream.SearchProcessorConfig{
		Indexer: search.IndexerConfig{
			BatchSize:      viper.GetInt("PGSTREAM_SEARCH_INDEXER_BATCH_SIZE"),
			BatchTime:      viper.GetDuration("PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT"),
			MaxQueueBytes:  viper.GetInt64("PGSTREAM_SEARCH_INDEXER_MAX_QUEUE_BYTES"),
			CleanupBackoff: parseBackoffConfig("PGSTREAM_SEARCH_INDEXER_CLEANUP"),
		},
		Store: opensearch.Config{
			URL: searchStore,
		},
		Retrier: &search.StoreRetryConfig{
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

func parseTranslatorConfig() *translator.Config {
	pgURL := viper.GetString("PGSTREAM_TRANSLATOR_STORE_POSTGRES_URL")
	if pgURL == "" {
		return nil
	}
	return &translator.Config{
		Store: pgschemalog.Config{
			URL: pgURL,
		},
	}
}

func parseTLSConfig(prefix string) *tls.Config {
	return &tls.Config{
		Enabled:        viper.GetBool(fmt.Sprintf("%s_TLS_ENABLED", prefix)),
		CaCertFile:     viper.GetString(fmt.Sprintf("%s_TLS_CA_CERT_FILE", prefix)),
		ClientCertFile: viper.GetString(fmt.Sprintf("%s_TLS_CLIENT_CERT_FILE", prefix)),
		ClientKeyFile:  viper.GetString(fmt.Sprintf("%s_TLS_CLIENT_KEY_FILE", prefix)),
	}
}
