// SPDX-License-Identifier: Apache-2.0

package stream

import (
	kafkalib "github.com/xataio/pgstream/internal/kafka"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	pglistener "github.com/xataio/pgstream/pkg/wal/listener/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/opensearch"
	"github.com/xataio/pgstream/pkg/wal/processor/translator"
)

type Config struct {
	PostgresURL string
	Listener    *ListenerConfig
	KafkaWriter *kafkalib.WriterConfig
	KafkaReader *kafkalistener.ReaderConfig
	Search      *SearchConfig
}

type ListenerConfig struct {
	Postgres   pglistener.Config
	Translator translator.Config
}

type SearchConfig struct {
	Indexer search.IndexerConfig
	Store   opensearch.Config
}
