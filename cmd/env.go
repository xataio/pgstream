// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/viper"
	"github.com/xataio/pgstream/internal/backoff"
	"github.com/xataio/pgstream/internal/kafka"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/stream"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	"github.com/xataio/pgstream/pkg/wal/listener/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/opensearch"
	"github.com/xataio/pgstream/pkg/wal/processor/translator"
)

type Env struct {
	PostgresURL             string        `mapstructure:"PGSTREAM_POSTGRES_URL"`
	ReplicationSyncInterval time.Duration `mapstructure:"PGSTREAM_REPLICATION_HANDLER_SYNC_LSN_INTERVAL"`

	KafkaServers                []string `mapstructure:"PGSTREAM_KAFKA_SERVERS"`
	KafkaTopicName              string   `mapstructure:"PGSTREAM_KAFKA_TOPIC_NAME"`
	KafkaTopicPartitions        int      `mapstructure:"PGSTREAM_KAFKA_TOPIC_PARTITIONS"`
	KafkaTopicReplicationFactor int      `mapstructure:"PGSTREAM_KAFKA_TOPIC_REPLICATION_FACTOR"`
	KafkaTopicAutoCreate        bool     `mapstructure:"PGSTREAM_KAFKA_TOPIC_AUTO_CREATE"`

	KafkaWriterBatchSize    int           `mapstructure:"PGSTREAM_KAFKA_WRITER_BATCH_SIZE"`
	KafkaWriterBatchTimeout time.Duration `mapstructure:"PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT"`
	KafkaWriterBatchBytes   int           `mapstructure:"PGSTREAM_KAFKA_WRITER_BATCH_BYTES"`

	KafkaReaderConsumerGroupID              string        `mapstructure:"PGSTREAM_KAFKA_READER_CONSUMER_GROUP_ID"`
	KafkaReaderConsumerGroupStartOffset     string        `mapstructure:"PGSTREAM_KAFKA_READER_CONSUMER_GROUP_START_OFFSET"`
	KafkaReaderCommitBackoffInitialInterval time.Duration `mapstructure:"PGSTREAM_KAFKA_READER_COMMIT_BACKOFF_INITIAL_INTERVAL"`
	KafkaReaderCommitBackoffMaxInterval     time.Duration `mapstructure:"PGSTREAM_KAFKA_READER_COMMIT_BACKOFF_MAX_INTERVAL"`
	KafkaReaderCommitBackoffMaxRetries      int           `mapstructure:"PGSTREAM_KAFKA_READER_COMMIT_BACKOFF_MAX_RETRIES"`

	SearchIndexerBatchSize                     int           `mapstructure:"PGSTREAM_SEARCH_INDEXER_BATCH_SIZE"`
	SearchIndexerBatchTimeout                  time.Duration `mapstructure:"PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT"`
	SearchIndexerCleanupBackoffInitialInterval time.Duration `mapstructure:"PGSTREAM_SEARCH_INDEXER_CLEANUP_BACKOFF_INITIAL_INTERVAL"`
	SearchIndexerCleanupBackoffMaxInterval     time.Duration `mapstructure:"PGSTREAM_SEARCH_INDEXER_CLEANUP_BACKOFF_MAX_INTERVAL"`
	SearchIndexerCleanupBackoffMaxRetries      int           `mapstructure:"PGSTREAM_SEARCH_INDEXER_CLEANUP_BACKOFF_MAX_RETRIES"`
	SearchStoreURL                             string        `mapstructure:"PGSTREAM_SEARCH_STORE_URL"`
}

func loadEnv() *Env {
	viper.SetConfigFile(".env")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(err)
	}

	env := Env{}
	if err := viper.Unmarshal(&env); err != nil {
		log.Fatal(err)
	}

	return &env
}

func parseStreamConfig() *stream.Config {
	listenerCfg, err := parseListenerConfig()
	if err != nil {
		log.Fatal(err)
	}

	return &stream.Config{
		PostgresURL: env.PostgresURL,
		Listener:    listenerCfg,
		KafkaWriter: parseKafkaWriterConfig(),
		KafkaReader: parseKafkaReaderConfig(),
		Search:      parseSearchConfig(),
	}
}

func parseListenerConfig() (*stream.ListenerConfig, error) {
	pgCfg, err := pgx.ParseConfig(env.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing postgres config: %w", err)
	}

	return &stream.ListenerConfig{
		Postgres: postgres.Config{
			Conn:            *pgCfg,
			SyncLSNInterval: env.ReplicationSyncInterval,
		},
		Translator: translator.Config{
			Store: schemalogpg.Config{
				Postgres: pgCfg,
			},
		},
	}, nil
}

func parseKafkaWriterConfig() *kafka.WriterConfig {
	if len(env.KafkaServers) == 0 || env.KafkaTopicName == "" {
		return nil
	}
	return &kafka.WriterConfig{
		Conn: kafka.ConnConfig{
			Servers: env.KafkaServers,
			Topic: kafka.TopicConfig{
				Name:              env.KafkaTopicName,
				NumPartitions:     env.KafkaTopicPartitions,
				ReplicationFactor: env.KafkaTopicReplicationFactor,
				AutoCreate:        env.KafkaTopicAutoCreate,
			},
			TLS: &kafka.TLSConfig{
				// TODO: add support for TLS configuration
				Enabled: false,
			},
		},
		BatchTimeout: env.KafkaWriterBatchTimeout,
		BatchBytes:   int64(env.KafkaWriterBatchBytes),
		BatchSize:    env.KafkaWriterBatchSize,
	}
}

func parseKafkaReaderConfig() *kafkalistener.ReaderConfig {
	if len(env.KafkaServers) == 0 || env.KafkaTopicName == "" || env.KafkaReaderConsumerGroupID == "" {
		return nil
	}

	return &kafkalistener.ReaderConfig{
		Kafka: kafka.ReaderConfig{
			Conn: kafka.ConnConfig{
				Servers: env.KafkaServers,
				Topic: kafka.TopicConfig{
					Name: env.KafkaTopicName,
				},
				TLS: &kafka.TLSConfig{
					// TODO: add support for TLS configuration
					Enabled: false,
				},
			},
			ConsumerGroupID:          env.KafkaReaderConsumerGroupID,
			ConsumerGroupStartOffset: env.KafkaReaderConsumerGroupStartOffset,
		},
		CommitBackoff: backoff.Config{
			InitialInterval: env.KafkaReaderCommitBackoffInitialInterval,
			MaxInterval:     env.KafkaReaderCommitBackoffMaxInterval,
			MaxRetries:      uint(env.KafkaReaderCommitBackoffMaxRetries),
		},
	}
}

func parseSearchConfig() *stream.SearchConfig {
	if env.SearchStoreURL == "" {
		return nil
	}

	return &stream.SearchConfig{
		Indexer: search.IndexerConfig{
			BatchSize: env.SearchIndexerBatchSize,
			BatchTime: env.SearchIndexerBatchTimeout,
			CleanupBackoff: backoff.Config{
				InitialInterval: env.SearchIndexerCleanupBackoffInitialInterval,
				MaxInterval:     env.SearchIndexerCleanupBackoffMaxInterval,
				MaxRetries:      uint(env.SearchIndexerCleanupBackoffMaxRetries),
			},
		},
		Store: opensearch.Config{
			URL: env.SearchStoreURL,
		},
	}
}
