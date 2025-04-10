// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgstream/pkg/stream"
)

// this function validates the stream configuration produced from the test
// configuration in the test directory.
func validateTestStreamConfig(t *testing.T, streamConfig *stream.Config) {
	assert.NotNil(t, streamConfig.Listener)
	assert.NotNil(t, streamConfig.Listener.Postgres)
	assert.Equal(t, "postgresql://user:password@localhost:5432/mydatabase", streamConfig.Listener.Postgres.Replication.PostgresURL)
	assert.Equal(t, "pgstream_mydatabase_slot", streamConfig.Listener.Postgres.Replication.ReplicationSlotName)
	assert.NotNil(t, streamConfig.Listener.Postgres.Snapshot)
	assert.ElementsMatch(t, []string{"test", "test_schema.test", "another_schema.*"}, streamConfig.Listener.Postgres.Snapshot.Adapter.Tables)
	assert.Equal(t, uint(4), streamConfig.Listener.Postgres.Snapshot.Adapter.SnapshotWorkers)
	assert.Equal(t, uint(4), streamConfig.Listener.Postgres.Snapshot.Generator.SchemaWorkers)
	assert.Equal(t, uint(4), streamConfig.Listener.Postgres.Snapshot.Generator.TableWorkers)
	assert.Equal(t, uint(1000), streamConfig.Listener.Postgres.Snapshot.Generator.BatchPageSize)
	assert.NotNil(t, streamConfig.Listener.Postgres.Snapshot.Schema.DumpRestore)
	assert.True(t, streamConfig.Listener.Postgres.Snapshot.Schema.DumpRestore.CleanTargetDB)
	assert.NotNil(t, streamConfig.Listener.Postgres.Snapshot.Recorder)
	assert.Equal(t, "postgresql://user:password@localhost:5432/mytargetdatabase", streamConfig.Listener.Postgres.Snapshot.Recorder.SnapshotStoreURL)
	assert.True(t, streamConfig.Listener.Postgres.Snapshot.Recorder.RepeatableSnapshots)

	assert.NotNil(t, streamConfig.Listener.Kafka)
	assert.Equal(t, []string{"localhost:9092"}, streamConfig.Listener.Kafka.Reader.Conn.Servers)
	assert.Equal(t, "mytopic", streamConfig.Listener.Kafka.Reader.Conn.Topic.Name)
	assert.Equal(t, "mygroup", streamConfig.Listener.Kafka.Reader.ConsumerGroupID)
	assert.Equal(t, "earliest", streamConfig.Listener.Kafka.Reader.ConsumerGroupStartOffset)
	assert.True(t, streamConfig.Listener.Kafka.Reader.Conn.TLS.Enabled)
	assert.Equal(t, "/path/to/ca.crt", streamConfig.Listener.Kafka.Reader.Conn.TLS.CaCertFile)
	assert.Equal(t, "/path/to/client.crt", streamConfig.Listener.Kafka.Reader.Conn.TLS.ClientCertFile)
	assert.Equal(t, "/path/to/client.key", streamConfig.Listener.Kafka.Reader.Conn.TLS.ClientKeyFile)
	assert.Equal(t, uint(5), streamConfig.Listener.Kafka.Checkpointer.CommitBackoff.Exponential.MaxRetries)
	assert.Equal(t, time.Second, streamConfig.Listener.Kafka.Checkpointer.CommitBackoff.Exponential.InitialInterval)
	assert.Equal(t, 60*time.Second, streamConfig.Listener.Kafka.Checkpointer.CommitBackoff.Exponential.MaxInterval)

	assert.NotNil(t, streamConfig.Processor.Postgres)
	assert.Equal(t, "postgresql://user:password@localhost:5432/mytargetdatabase", streamConfig.Processor.Postgres.BatchWriter.URL)
	assert.Equal(t, int64(100), streamConfig.Processor.Postgres.BatchWriter.BatchConfig.MaxBatchSize)
	assert.Equal(t, time.Second, streamConfig.Processor.Postgres.BatchWriter.BatchConfig.BatchTimeout)
	assert.Equal(t, int64(1572864), streamConfig.Processor.Postgres.BatchWriter.BatchConfig.MaxBatchBytes)
	assert.Equal(t, int64(204800), streamConfig.Processor.Postgres.BatchWriter.BatchConfig.MaxQueueBytes)
	assert.False(t, streamConfig.Processor.Postgres.BatchWriter.DisableTriggers)
	assert.Equal(t, "nothing", streamConfig.Processor.Postgres.BatchWriter.OnConflictAction)

	assert.NotNil(t, streamConfig.Processor.Kafka)
	assert.Equal(t, "mytopic", streamConfig.Processor.Kafka.Writer.Kafka.Topic.Name)
	assert.Equal(t, 1, streamConfig.Processor.Kafka.Writer.Kafka.Topic.NumPartitions)
	assert.Equal(t, 1, streamConfig.Processor.Kafka.Writer.Kafka.Topic.ReplicationFactor)
	assert.True(t, streamConfig.Processor.Kafka.Writer.Kafka.Topic.AutoCreate)
	assert.Equal(t, []string{"localhost:9092"}, streamConfig.Processor.Kafka.Writer.Kafka.Servers)
	assert.Equal(t, int64(100), streamConfig.Processor.Kafka.Writer.Batch.MaxBatchSize)
	assert.Equal(t, time.Second, streamConfig.Processor.Kafka.Writer.Batch.BatchTimeout)
	assert.Equal(t, int64(1572864), streamConfig.Processor.Kafka.Writer.Batch.MaxBatchBytes)
	assert.Equal(t, int64(204800), streamConfig.Processor.Kafka.Writer.Batch.MaxQueueBytes)
	assert.True(t, streamConfig.Processor.Kafka.Writer.Kafka.TLS.Enabled)
	assert.Equal(t, "/path/to/ca.crt", streamConfig.Processor.Kafka.Writer.Kafka.TLS.CaCertFile)
	assert.Equal(t, "/path/to/client.crt", streamConfig.Processor.Kafka.Writer.Kafka.TLS.ClientCertFile)
	assert.Equal(t, "/path/to/client.key", streamConfig.Processor.Kafka.Writer.Kafka.TLS.ClientKeyFile)

	assert.NotNil(t, streamConfig.Processor.Search)
	assert.Equal(t, "http://localhost:9200", streamConfig.Processor.Search.Store.ElasticsearchURL)
	assert.Equal(t, int64(100), streamConfig.Processor.Search.Indexer.Batch.MaxBatchSize)
	assert.Equal(t, time.Second, streamConfig.Processor.Search.Indexer.Batch.BatchTimeout)
	assert.Equal(t, int64(1572864), streamConfig.Processor.Search.Indexer.Batch.MaxBatchBytes)
	assert.Equal(t, int64(204800), streamConfig.Processor.Search.Indexer.Batch.MaxQueueBytes)
	assert.Equal(t, uint(5), streamConfig.Processor.Search.Retrier.Backoff.Exponential.MaxRetries)
	assert.Equal(t, time.Second, streamConfig.Processor.Search.Retrier.Backoff.Exponential.InitialInterval)
	assert.Equal(t, 60*time.Second, streamConfig.Processor.Search.Retrier.Backoff.Exponential.MaxInterval)

	assert.NotNil(t, streamConfig.Processor.Webhook)
	assert.Equal(t, "localhost:9090", streamConfig.Processor.Webhook.SubscriptionServer.Address)
	assert.Equal(t, uint(4), streamConfig.Processor.Webhook.Notifier.URLWorkerCount)
	assert.Equal(t, "postgresql://user:password@localhost:5432/mydatabase", streamConfig.Processor.Webhook.SubscriptionStore.URL)
	assert.True(t, streamConfig.Processor.Webhook.SubscriptionStore.CacheEnabled)
	assert.Equal(t, 60*time.Second, streamConfig.Processor.Webhook.SubscriptionStore.CacheRefreshInterval)
	assert.Equal(t, time.Minute, streamConfig.Processor.Webhook.SubscriptionServer.ReadTimeout)
	assert.Equal(t, time.Minute, streamConfig.Processor.Webhook.SubscriptionServer.WriteTimeout)
	assert.Equal(t, time.Second, streamConfig.Processor.Webhook.Notifier.ClientTimeout)

	assert.NotNil(t, streamConfig.Processor.Injector)
	assert.Equal(t, "postgresql://user:password@localhost:5432/mydatabase", streamConfig.Processor.Injector.Store.URL)

	assert.NotNil(t, streamConfig.Processor.Transformer)
	assert.Len(t, streamConfig.Processor.Transformer.TransformerRules, 1)
	assert.Equal(t, "public", streamConfig.Processor.Transformer.TransformerRules[0].Schema)
	assert.Equal(t, "test", streamConfig.Processor.Transformer.TransformerRules[0].Table)
	assert.NotNil(t, streamConfig.Processor.Transformer.TransformerRules[0].ColumnRules)
	assert.NotNil(t, streamConfig.Processor.Transformer.TransformerRules[0].ColumnRules["name"])
	assert.Equal(t, "greenmask_firstname", streamConfig.Processor.Transformer.TransformerRules[0].ColumnRules["name"].Name)
	assert.NotNil(t, streamConfig.Processor.Transformer.TransformerRules[0].ColumnRules["name"].DynamicParameters)
	assert.NotNil(t, streamConfig.Processor.Transformer.TransformerRules[0].ColumnRules["name"].DynamicParameters["gender"])
	dynParamGender, ok := streamConfig.Processor.Transformer.TransformerRules[0].ColumnRules["name"].DynamicParameters["gender"].(map[string]any)
	assert.True(t, ok)
	column, ok := dynParamGender["column"].(string)
	assert.True(t, ok)
	assert.Equal(t, "sex", column)
}
