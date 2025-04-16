// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_EnvConfigToStreamConfig(t *testing.T) {
	require.NoError(t, LoadFile("test/test_config.env"))

	streamConfig, err := envConfigToStreamConfig()
	assert.NoError(t, err)
	assert.NotNil(t, streamConfig)

	validateTestStreamConfig(t, streamConfig)
}

func Test_EnvVarsToStreamConfig(t *testing.T) {
	os.Setenv("PGSTREAM_POSTGRES_LISTENER_URL", "postgresql://user:password@localhost:5432/mydatabase")
	os.Setenv("PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME", "pgstream_mydatabase_slot")
	os.Setenv("PGSTREAM_POSTGRES_LISTENER_INITIAL_SNAPSHOT_ENABLED", "true")
	os.Setenv("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_STORE_URL", "postgresql://user:password@localhost:5432/mytargetdatabase")
	os.Setenv("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_STORE_REPEATABLE", "true")
	os.Setenv("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLES", "test test_schema.test another_schema.*")
	os.Setenv("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_WORKERS", "4")
	os.Setenv("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_SCHEMA_WORKERS", "4")
	os.Setenv("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLE_WORKERS", "4")
	os.Setenv("PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_BATCH_PAGE_SIZE", "1000")
	os.Setenv("PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB", "true")

	os.Setenv("PGSTREAM_KAFKA_SERVERS", "localhost:9092")
	os.Setenv("PGSTREAM_KAFKA_TOPIC_NAME", "mytopic")
	os.Setenv("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_ID", "mygroup")
	os.Setenv("PGSTREAM_KAFKA_READER_CONSUMER_GROUP_START_OFFSET", "earliest")
	os.Setenv("PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_INITIAL_INTERVAL", "1s")
	os.Setenv("PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_MAX_INTERVAL", "1m")
	os.Setenv("PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_MAX_RETRIES", "5")
	os.Setenv("PGSTREAM_KAFKA_TLS_ENABLED", "true")
	os.Setenv("PGSTREAM_KAFKA_TLS_CA_CERT_FILE", "/path/to/ca.crt")
	os.Setenv("PGSTREAM_KAFKA_TLS_CLIENT_CERT_FILE", "/path/to/client.crt")
	os.Setenv("PGSTREAM_KAFKA_TLS_CLIENT_KEY_FILE", "/path/to/client.key")

	os.Setenv("PGSTREAM_POSTGRES_WRITER_TARGET_URL", "postgresql://user:password@localhost:5432/mytargetdatabase")
	os.Setenv("PGSTREAM_POSTGRES_WRITER_BATCH_SIZE", "100")
	os.Setenv("PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT", "1s")
	os.Setenv("PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES", "204800")
	os.Setenv("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES", "1572864")
	os.Setenv("PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL", "postgresql://user:password@localhost:5432/mydatabase")
	os.Setenv("PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS", "false")
	os.Setenv("PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION", "nothing")

	os.Setenv("PGSTREAM_KAFKA_TOPIC_PARTITIONS", "1")
	os.Setenv("PGSTREAM_KAFKA_TOPIC_REPLICATION_FACTOR", "1")
	os.Setenv("PGSTREAM_KAFKA_TOPIC_AUTO_CREATE", "true")
	os.Setenv("PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT", "1s")
	os.Setenv("PGSTREAM_KAFKA_WRITER_BATCH_SIZE", "100")
	os.Setenv("PGSTREAM_KAFKA_WRITER_BATCH_BYTES", "1572864")
	os.Setenv("PGSTREAM_KAFKA_WRITER_MAX_QUEUE_BYTES", "204800")

	os.Setenv("PGSTREAM_SEARCH_INDEXER_BATCH_SIZE", "100")
	os.Setenv("PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT", "1s")
	os.Setenv("PGSTREAM_SEARCH_INDEXER_MAX_QUEUE_BYTES", "204800")
	os.Setenv("PGSTREAM_SEARCH_INDEXER_BATCH_BYTES", "1572864")
	os.Setenv("PGSTREAM_ELASTICSEARCH_STORE_URL", "http://localhost:9200")
	os.Setenv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_INITIAL_INTERVAL", "1s")
	os.Setenv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_INTERVAL", "1m")
	os.Setenv("PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_RETRIES", "5")

	os.Setenv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_URL", "postgresql://user:password@localhost:5432/mydatabase")
	os.Setenv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_ENABLED", "true")
	os.Setenv("PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_REFRESH_INTERVAL", "60s")
	os.Setenv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_ADDRESS", "localhost:9090")
	os.Setenv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_READ_TIMEOUT", "60s")
	os.Setenv("PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_WRITE_TIMEOUT", "60s")
	os.Setenv("PGSTREAM_WEBHOOK_NOTIFIER_WORKER_COUNT", "4")
	os.Setenv("PGSTREAM_WEBHOOK_NOTIFIER_CLIENT_TIMEOUT", "1s")

	os.Setenv("PGSTREAM_INJECTOR_STORE_POSTGRES_URL", "postgresql://user:password@localhost:5432/mydatabase")
	os.Setenv("PGSTREAM_TRANSFORMER_RULES_FILE", "test/test_transformer_rules.yaml")

	streamConfig, err := envConfigToStreamConfig()
	assert.NoError(t, err)
	assert.NotNil(t, streamConfig)

	validateTestStreamConfig(t, streamConfig)
}
