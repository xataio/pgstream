// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/log/zerolog"
	pglib "github.com/xataio/pgstream/internal/postgres"
	kafkalib "github.com/xataio/pgstream/pkg/kafka"
	loglib "github.com/xataio/pgstream/pkg/log"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/tls"
	"github.com/xataio/pgstream/pkg/wal"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

var (
	pgurl            string
	targetPGURL      string
	kafkaBrokers     []string
	opensearchURL    string
	elasticsearchURL string
)

type mockProcessor struct {
	eventChan chan *wal.Event
}

func (mp *mockProcessor) process(ctx context.Context, event *wal.Event) error {
	mp.eventChan <- event
	return nil
}

func (mp *mockProcessor) close() {
	close(mp.eventChan)
}

type mockWebhookServer struct {
	*httptest.Server
	dataChan chan *wal.Data
}

func newMockWebhookServer() *mockWebhookServer {
	mw := &mockWebhookServer{
		dataChan: make(chan *wal.Data),
	}
	mw.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := webhook.Payload{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mw.dataChan <- payload.Data
	}))
	return mw
}

func (m *mockWebhookServer) close() {
	close(m.dataChan)
	m.Close()
}

func runStream(t *testing.T, ctx context.Context, cfg *stream.Config) {
	// start the configured stream listener/processor
	go func() {
		err := stream.Run(ctx, testLogger(), cfg, nil)
		require.NoError(t, err)
	}()
}

func initStream(t *testing.T, ctx context.Context, url string) {
	err := stream.Init(ctx, url, "")
	require.NoError(t, err)
}

func execQuery(t *testing.T, ctx context.Context, query string) {
	execQueryWithURL(t, ctx, pgurl, query)
}

func execQueryWithURL(t *testing.T, ctx context.Context, url, query string) {
	conn, err := pglib.NewConn(ctx, url)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, query)
	require.NoError(t, err)
}

func testLogger() loglib.Logger {
	return zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: "trace",
	}))
}

func testPostgresListenerCfg() stream.ListenerConfig {
	return stream.ListenerConfig{
		Postgres: &stream.PostgresListenerConfig{
			Replication: pgreplication.Config{
				PostgresURL: pgurl,
			},
		},
	}
}

func testSnapshotListenerCfg(sourceURL, targetURL string, tables []string) stream.ListenerConfig {
	return stream.ListenerConfig{
		Snapshot: &snapshotbuilder.SnapshotListenerConfig{
			Generator: pgsnapshotgenerator.Config{
				URL: sourceURL,
			},
			Adapter: adapter.SnapshotConfig{
				Tables: tables,
			},
			Schema: snapshotbuilder.SchemaSnapshotConfig{
				DumpRestore: &pgdumprestore.Config{
					SourcePGURL: sourceURL,
					TargetPGURL: targetURL,
				},
			},
		},
	}
}

func testPostgresListenerCfgWithSnapshot(sourceURL, targetURL string, tables []string) stream.ListenerConfig {
	return stream.ListenerConfig{
		Postgres: &stream.PostgresListenerConfig{
			Replication: pgreplication.Config{
				PostgresURL: sourceURL,
			},
			Snapshot: &snapshotbuilder.SnapshotListenerConfig{
				Generator: pgsnapshotgenerator.Config{
					URL: sourceURL,
				},
				Adapter: adapter.SnapshotConfig{
					Tables: tables,
				},
				Schema: snapshotbuilder.SchemaSnapshotConfig{
					DumpRestore: &pgdumprestore.Config{
						SourcePGURL: sourceURL,
						TargetPGURL: targetURL,
					},
				},
			},
		},
	}
}

func testKafkaListenerCfg() stream.ListenerConfig {
	return stream.ListenerConfig{
		Kafka: &stream.KafkaListenerConfig{
			Reader: kafkalib.ReaderConfig{
				Conn:            testKafkaCfg(),
				ConsumerGroupID: "integration-test-group",
			},
			Checkpointer: kafkacheckpoint.Config{},
		},
	}
}

func testKafkaProcessorCfg() stream.ProcessorConfig {
	return stream.ProcessorConfig{
		Kafka: &stream.KafkaProcessorConfig{
			Writer: &kafkaprocessor.Config{
				Kafka: testKafkaCfg(),
			},
		},
		Injector: &injector.Config{
			Store: schemalogpg.Config{
				URL: pgurl,
			},
		},
	}
}

func testSearchProcessorCfg(storeCfg store.Config) stream.ProcessorConfig {
	return stream.ProcessorConfig{
		Search: &stream.SearchProcessorConfig{
			Store: storeCfg,
		},
		Injector: &injector.Config{
			Store: schemalogpg.Config{
				URL: pgurl,
			},
		},
	}
}

func testWebhookProcessorCfg() stream.ProcessorConfig {
	return stream.ProcessorConfig{
		Webhook: &stream.WebhookProcessorConfig{
			Notifier: notifier.Config{},
			SubscriptionStore: stream.WebhookSubscriptionStoreConfig{
				URL: pgurl,
			},
		},
		Injector: &injector.Config{
			Store: schemalogpg.Config{
				URL: pgurl,
			},
		},
	}
}

func testPostgresProcessorCfg(sourcePGURL string) stream.ProcessorConfig {
	return stream.ProcessorConfig{
		Postgres: &stream.PostgresProcessorConfig{
			BatchWriter: postgres.Config{
				URL: targetPGURL,
				BatchConfig: batch.Config{
					BatchTimeout: 50 * time.Millisecond,
				},
				SchemaLogStore: schemalogpg.Config{
					URL: sourcePGURL,
				},
			},
		},
		Injector: &injector.Config{
			Store: schemalogpg.Config{
				URL: sourcePGURL,
			},
		},
	}
}

func testPostgresProcessorCfgWithTransformer(sourcePGURL string) stream.ProcessorConfig {
	return stream.ProcessorConfig{
		Postgres: &stream.PostgresProcessorConfig{
			BatchWriter: postgres.Config{
				URL: targetPGURL,
				BatchConfig: batch.Config{
					BatchTimeout: 50 * time.Millisecond,
				},
				SchemaLogStore: schemalogpg.Config{
					URL: sourcePGURL,
				},
			},
		},
		Injector: &injector.Config{
			Store: schemalogpg.Config{
				URL: sourcePGURL,
			},
		},
		Transformer: &transformer.Config{
			TransformerRules: testTransformationRules(),
		},
	}
}

func testKafkaCfg() kafkalib.ConnConfig {
	return kafkalib.ConnConfig{
		Servers: kafkaBrokers,
		Topic: kafkalib.TopicConfig{
			Name:       "integration-tests",
			AutoCreate: true,
		},
		TLS: tls.Config{
			Enabled: false,
		},
	}
}

func testTransformationRules() []transformer.TableRules {
	return []transformer.TableRules{
		{
			Schema: "public",
			Table:  "pg2pg_integration_transformer_test",
			ColumnRules: map[string]transformer.TransformerRules{
				"name": {
					Name: "neosync_firstname",
					Parameters: map[string]any{
						"preserve_length": false,
						"max_length":      5,
					},
				},
				"last_name": {
					Name: "neosync_string",
					Parameters: map[string]any{
						"preserve_length": false,
						"max_length":      10,
					},
				},
				"email": {
					Name: "neosync_email",
					Parameters: map[string]any{
						"preserve_length":  false,
						"max_length":       15,
						"excluded_domains": []string{"example.com", "example.net"},
					},
				},
				"secondary_email": {
					Name: "masking",
					Parameters: map[string]any{
						"type": "email",
					},
				},
				"address": {
					Name: "greenmask_string",
					Parameters: map[string]any{
						"preserve_length": false,
						"max_length":      20,
					},
				},
				"age": {
					Name: "greenmask_integer",
					Parameters: map[string]any{
						"generator": "deterministic",
						"min_value": 18,
						"max_value": 75,
					},
				},
				"total_purchases": {
					Name: "greenmask_float",
					Parameters: map[string]any{
						"generator": "deterministic",
						"min_value": 0.0,
						"max_value": 1000.0,
					},
				},
				"customer_id": {
					Name: "greenmask_uuid",
				},
				"birth_date": {
					Name: "greenmask_date",
					Parameters: map[string]any{
						"min_value": "1990-01-01",
						"max_value": "2000-12-31",
					},
				},
				"is_active": {
					Name: "greenmask_boolean",
				},
				"created_at": {
					Name: "greenmask_unix_timestamp",
					Parameters: map[string]any{
						"min_value": "1741856058",
						"max_value": "1741956058",
					},
				},
				"updated_at": {
					Name: "greenmask_utc_timestamp",
					Parameters: map[string]any{
						"min_timestamp": "2022-01-01T00:00:00Z",
						"max_timestamp": "2024-01-01T23:59:59Z",
					},
				},
				"gender": {
					Name: "greenmask_choice",
					Parameters: map[string]any{
						"choices": []string{"M", "F", "None"},
					},
				},
			},
		},
	}
}
