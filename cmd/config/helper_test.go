// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/otel"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/tls"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
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

// this function validates the stream configuration produced from the test
// configuration in the test directory.
func validateTestStreamConfig(t *testing.T, streamConfig *stream.Config) {
	expectedConfig := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: "postgresql://user:password@localhost:5432/mydatabase",
				Replication: pgreplication.Config{
					PostgresURL:         "postgresql://user:password@localhost:5432/mydatabase",
					ReplicationSlotName: "pgstream_mydatabase_slot",
					IncludeTables:       []string{"test", "test_schema.test", "another_schema.*"},
					ExcludeTables:       []string{"excluded_test", "excluded_schema.test", "another_excluded_schema.*"},
				},
				RetryPolicy: backoff.Config{
					Exponential: &backoff.ExponentialConfig{
						MaxRetries:      5,
						InitialInterval: time.Second,
						MaxInterval:     60 * time.Second,
					},
				},
				Snapshot: &builder.SnapshotListenerConfig{
					Adapter: adapter.SnapshotConfig{
						Tables:         []string{"test", "test_schema.Test", "another_schema.*"},
						ExcludedTables: []string{"test_schema.Test"},
					},
					Generator: pgsnapshotgenerator.Config{
						URL:             "postgresql://user:password@localhost:5432/mydatabase",
						SnapshotWorkers: 4,
						SchemaWorkers:   4,
						TableWorkers:    4,
						BatchBytes:      83886080,
						MaxConnections:  20,
					},
					Schema: builder.SchemaSnapshotConfig{
						DumpRestore: &pgdumprestore.Config{
							SourcePGURL:            "postgresql://user:password@localhost:5432/mydatabase",
							TargetPGURL:            "postgresql://user:password@localhost:5432/mytargetdatabase",
							CleanTargetDB:          true,
							CreateTargetDB:         true,
							IncludeGlobalDBObjects: true,
							RolesSnapshotMode:      "disabled",
							Role:                   "test-role",
							NoOwner:                true,
							NoPrivileges:           true,
							DumpDebugFile:          "pg_dump.sql",
							ExcludedSecurityLabels: []string{"anon"},
						},
					},
					Recorder: &builder.SnapshotRecorderConfig{
						SnapshotStoreURL:    "postgresql://user:password@localhost:5432/mytargetdatabase",
						RepeatableSnapshots: true,
					},
				},
			},
			Kafka: &stream.KafkaListenerConfig{
				Reader: kafka.ReaderConfig{
					Conn: kafka.ConnConfig{
						Servers: []string{"localhost:9092"},
						Topic: kafka.TopicConfig{
							Name: "mytopic",
						},
						TLS: tls.Config{
							Enabled:        true,
							CaCertFile:     "/path/to/ca.crt",
							ClientCertFile: "/path/to/client.crt",
							ClientKeyFile:  "/path/to/client.key",
						},
					},
					ConsumerGroupID:          "mygroup",
					ConsumerGroupStartOffset: "earliest",
				},
				Checkpointer: kafkacheckpoint.Config{
					CommitBackoff: backoff.Config{
						Exponential: &backoff.ExponentialConfig{
							MaxRetries:      5,
							InitialInterval: time.Second,
							MaxInterval:     60 * time.Second,
						},
					},
				},
			},
		},
		Processor: stream.ProcessorConfig{
			Postgres: &stream.PostgresProcessorConfig{
				BatchWriter: postgres.Config{
					URL: "postgresql://user:password@localhost:5432/mytargetdatabase",
					BatchConfig: batch.Config{
						MaxBatchSize:     100,
						BatchTimeout:     time.Second,
						MaxBatchBytes:    1572864,
						MaxQueueBytes:    204800,
						IgnoreSendErrors: true,
					},
					DisableTriggers:   false,
					OnConflictAction:  "nothing",
					BulkIngestEnabled: true,
					SchemaLogStore: schemalogpg.Config{
						URL: "postgresql://user:password@localhost:5432/mydatabase",
					},
				},
			},
			Kafka: &stream.KafkaProcessorConfig{
				Writer: &kafkaprocessor.Config{
					Kafka: kafka.ConnConfig{
						Topic: kafka.TopicConfig{
							Name:              "mytopic",
							NumPartitions:     1,
							ReplicationFactor: 1,
							AutoCreate:        true,
						},
						Servers: []string{"localhost:9092"},
						TLS: tls.Config{
							Enabled:        true,
							CaCertFile:     "/path/to/ca.crt",
							ClientCertFile: "/path/to/client.crt",
							ClientKeyFile:  "/path/to/client.key",
						},
					},
					Batch: batch.Config{
						MaxBatchSize:     100,
						BatchTimeout:     time.Second,
						MaxBatchBytes:    1572864,
						MaxQueueBytes:    204800,
						IgnoreSendErrors: true,
					},
				},
			},
			Search: &stream.SearchProcessorConfig{
				Store: store.Config{
					ElasticsearchURL: "http://localhost:9200",
				},
				Indexer: search.IndexerConfig{
					Batch: batch.Config{
						MaxBatchSize:     100,
						BatchTimeout:     time.Second,
						MaxBatchBytes:    1572864,
						MaxQueueBytes:    204800,
						IgnoreSendErrors: true,
					},
				},
				Retrier: search.StoreRetryConfig{
					Backoff: backoff.Config{
						Exponential: &backoff.ExponentialConfig{
							MaxRetries:      5,
							InitialInterval: time.Second,
							MaxInterval:     60 * time.Second,
						},
					},
				},
			},
			Webhook: &stream.WebhookProcessorConfig{
				SubscriptionServer: server.Config{
					Address:      "localhost:9090",
					ReadTimeout:  time.Minute,
					WriteTimeout: time.Minute,
				},
				Notifier: notifier.Config{
					URLWorkerCount: 4,
					ClientTimeout:  time.Second,
				},
				SubscriptionStore: stream.WebhookSubscriptionStoreConfig{
					URL:                  "postgresql://user:password@localhost:5432/mydatabase",
					CacheEnabled:         true,
					CacheRefreshInterval: 60 * time.Second,
				},
			},
			Injector: &injector.Config{
				Store: schemalogpg.Config{
					URL: "postgresql://user:password@localhost:5432/mydatabase",
				},
			},
			Transformer: &transformer.Config{
				InferFromSecurityLabels: false,
				DumpInferredRules:       false,
				ValidationMode:          "relaxed",
				TransformerRules: []transformer.TableRules{
					{
						Schema:         "public",
						Table:          "test",
						ValidationMode: "relaxed",
						ColumnRules: map[string]transformer.TransformerRules{
							"name": {
								Name: "greenmask_firstname",
								DynamicParameters: map[string]any{
									"gender": map[string]any{
										"column": "sex",
									},
								},
							},
						},
					},
				},
			},
			Filter: &filter.Config{
				IncludeTables: []string{"test", "test_schema.test", "another_schema.*"},
				ExcludeTables: []string{"excluded_test", "excluded_schema.test", "another_excluded_schema.*"},
			},
		},
	}

	assert.Equal(t, expectedConfig, streamConfig)
}

// this function validates the otel configuration produced from the test
// configuration in the test directory.
func validateTestOtelConfig(t *testing.T, otelConfig *otel.Config) {
	assert.Equal(t, "http://localhost:4317", otelConfig.Metrics.Endpoint)
	assert.Equal(t, 60*time.Second, otelConfig.Metrics.CollectionInterval)

	assert.Equal(t, "http://localhost:4317", otelConfig.Traces.Endpoint)
	assert.Equal(t, 0.5, otelConfig.Traces.SampleRatio)
}
