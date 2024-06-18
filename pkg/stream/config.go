// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"errors"

	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/opensearch"
	"github.com/xataio/pgstream/pkg/wal/processor/translator"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

type Config struct {
	Listener  ListenerConfig
	Processor ProcessorConfig
}

type ListenerConfig struct {
	Postgres *PostgresListenerConfig
	Kafka    *KafkaListenerConfig
}

type PostgresListenerConfig struct {
	Replication pgreplication.Config
}

type KafkaListenerConfig struct {
	Reader       kafkalistener.ReaderConfig
	Checkpointer kafkacheckpoint.Config
}

type ProcessorConfig struct {
	Kafka      *KafkaProcessorConfig
	Search     *SearchProcessorConfig
	Translator *translator.Config
}

type KafkaProcessorConfig struct {
	Writer *kafkaprocessor.Config
}

type SearchProcessorConfig struct {
	Indexer search.IndexerConfig
	Store   opensearch.Config
	Retrier *search.StoreRetryConfig
}

func (c *Config) IsValid() error {
	if c.Listener.Kafka == nil && c.Listener.Postgres == nil {
		return errors.New("need at least one listener configured")
	}

	if c.Processor.Kafka == nil && c.Processor.Search == nil {
		return errors.New("need at least one processor configured")
	}

	return nil
}
