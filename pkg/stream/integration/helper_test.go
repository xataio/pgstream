// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/log/zerolog"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/backoff"
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

const (
	withBulkIngestion    = true
	withoutBulkIngestion = false

	withGeneratedColumn = true
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
		err := stream.Run(ctx, testLogger(), cfg, false, nil)
		require.NoError(t, err)
	}()
}

func runSnapshot(t *testing.T, ctx context.Context, cfg *stream.Config) {
	// start the configured stream listener/processor
	go func() {
		err := stream.Snapshot(ctx, testLogger(), cfg, nil)
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
			URL: pgurl,
			Replication: pgreplication.Config{
				PostgresURL: pgurl,
			},
		},
	}
}

func testPostgresListenerCfgWithSnapshot(sourceURL, targetURL string, tables []string) stream.ListenerConfig {
	return stream.ListenerConfig{
		Postgres: &stream.PostgresListenerConfig{
			URL: sourceURL,
			Replication: pgreplication.Config{
				PostgresURL: sourceURL,
			},
			Snapshot: &snapshotbuilder.SnapshotListenerConfig{
				Data: &pgsnapshotgenerator.Config{
					URL: sourceURL,
				},
				Adapter: adapter.SnapshotConfig{
					Tables: tables,
				},
				Schema: &snapshotbuilder.SchemaSnapshotConfig{
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

func testPostgresProcessorCfg(sourcePGURL string, bulkIngestion bool) stream.ProcessorConfig {
	return stream.ProcessorConfig{
		Postgres: &stream.PostgresProcessorConfig{
			BatchWriter: postgres.Config{
				URL: targetPGURL,
				BatchConfig: batch.Config{
					MaxBatchSize: 1,
					// BatchTimeout: 50 * time.Millisecond,
				},
				SchemaLogStore: schemalogpg.Config{
					URL: sourcePGURL,
				},
				BulkIngestEnabled: bulkIngestion,
				RetryPolicy: backoff.Config{
					DisableRetries: true,
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
			Schema:         "public",
			Table:          "pg2pg_integration_transformer_test",
			ValidationMode: "strict",
			ColumnRules: map[string]transformer.TransformerRules{
				"id": {},
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
						"max_length": 20,
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

type constraintInfo struct {
	constraintType string
	definition     string
}

const tableConstraintsQuery = `
	SELECT con.conname,
			   CASE con.contype
					WHEN 'p' THEN 'PRIMARY KEY'
					WHEN 'u' THEN 'UNIQUE'
					WHEN 'f' THEN 'FOREIGN KEY'
					WHEN 'c' THEN 'CHECK'
					ELSE con.contype::text
			   END AS constraint_type,
			   pg_get_constraintdef(con.oid)
		FROM pg_constraint con
		JOIN pg_class rel ON rel.oid = con.conrelid
		JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
		WHERE nsp.nspname = $1 AND rel.relname = $2
`

func getTableConstraints(t *testing.T, ctx context.Context, conn pglib.Querier, schema, table string) map[string]constraintInfo {
	bo := backoff.NewConstantBackoff(ctx, &backoff.ConstantConfig{
		Interval:   200 * time.Millisecond,
		MaxRetries: 10,
	})

	var constraints map[string]constraintInfo
	err := bo.Retry(func() error {
		rows, err := conn.Query(ctx, tableConstraintsQuery, schema, table)
		if err != nil {
			return parseRetryError(err)
		}
		defer rows.Close()

		constraints = make(map[string]constraintInfo)
		for rows.Next() {
			var name, constraintType, definition string
			err := rows.Scan(&name, &constraintType, &definition)
			if err != nil {
				return parseRetryError(err)
			}
			constraints[name] = constraintInfo{
				constraintType: constraintType,
				definition:     definition,
			}
		}

		if err := rows.Err(); err != nil {
			return parseRetryError(err)
		}

		return nil
	})
	require.NoError(t, err)

	return constraints
}

const tableIndexesQuery = `
	SELECT indexname, indexdef
	FROM pg_indexes
	WHERE schemaname = $1 AND tablename = $2
`

func getTableIndexes(t *testing.T, ctx context.Context, conn pglib.Querier, schema, table string) map[string]string {
	bo := backoff.NewConstantBackoff(ctx, &backoff.ConstantConfig{
		Interval:   200 * time.Millisecond,
		MaxRetries: 10,
	})

	var indexes map[string]string
	err := bo.Retry(func() error {
		rows, err := conn.Query(ctx, tableIndexesQuery, schema, table)
		if err != nil {
			return parseRetryError(err)
		}
		defer rows.Close()

		indexes = make(map[string]string)
		for rows.Next() {
			var name string
			var def sql.NullString
			err := rows.Scan(&name, &def)
			if err != nil {
				return parseRetryError(err)
			}
			if !def.Valid {
				continue
			}
			indexes[name] = def.String
		}

		if err := rows.Err(); err != nil {
			return parseRetryError(err)
		}

		return nil
	})
	require.NoError(t, err)

	return indexes
}

func parseRetryError(err error) error {
	errCacheLookupFailed := &pglib.ErrCacheLookupFailed{}
	if errors.As(pglib.MapError(err), &errCacheLookupFailed) {
		return err
	}
	return fmt.Errorf("%w: %w", err, backoff.ErrPermanent)
}

type sequenceInfo struct {
	dataType     string
	increment    string
	minimumValue string
	maximumValue string
	startValue   string
	cycleOption  string
}

func getSequences(t *testing.T, ctx context.Context, conn pglib.Querier, schema string) map[string]sequenceInfo {
	rows, err := conn.Query(ctx, `
		SELECT sequence_name, data_type, increment, minimum_value, maximum_value, start_value, cycle_option
		FROM information_schema.sequences
		WHERE sequence_schema = $1
	`, schema)
	require.NoError(t, err)
	defer rows.Close()

	sequences := make(map[string]sequenceInfo)
	for rows.Next() {
		var name, dataType, increment, minValue, maxValue, startValue, cycleOption string
		err := rows.Scan(&name, &dataType, &increment, &minValue, &maxValue, &startValue, &cycleOption)
		require.NoError(t, err)
		sequences[name] = sequenceInfo{
			dataType:     dataType,
			increment:    increment,
			minimumValue: minValue,
			maximumValue: maxValue,
			startValue:   startValue,
			cycleOption:  cycleOption,
		}
	}
	require.NoError(t, rows.Err())

	return sequences
}
