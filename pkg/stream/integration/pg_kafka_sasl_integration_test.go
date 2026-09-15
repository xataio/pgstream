// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/testcontainers"
	kafkalib "github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
)

// Test_PostgresToKafka_SASL runs the postgres to kafka pipeline against a
// broker that rejects unauthenticated clients, once per supported mechanism.
// The writer authenticates through the kafka transport and the reader through
// the dialer, so a mechanism only passes here if both paths negotiate it.
func Test_PostgresToKafka_SASL(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	kafkaSASLContainer.require(t)

	mechanisms := []string{"plain", "scram-sha-256", "scram-sha-512"}
	for _, mechanism := range mechanisms {
		t.Run(mechanism, func(t *testing.T) {
			topic := "sasl-integration-tests-" + mechanism
			connCfg := testKafkaSASLCfg(topic, mechanism, kafkaSASLUser)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// the reader below joins its consumer group before the pipeline has
			// written anything, and a group that joins a topic which does not
			// exist yet is assigned no partitions and does not revisit that
			// assignment. Creating the topic up front settles the ordering.
			ensureKafkaTopic(t, connCfg)

			cfg := &stream.Config{
				Listener: testPostgresListenerCfg(t),
				Processor: stream.ProcessorConfig{
					Kafka: &stream.KafkaProcessorConfig{
						Writer: &kafkaprocessor.Config{Kafka: connCfg},
					},
					Injector: &injector.Config{URL: pgurl},
				},
			}
			runStream(t, ctx, cfg)

			mockProcessor := &mockProcessor{
				eventChan: make(chan *wal.Event),
				skipEventFn: func(event *wal.Event) bool {
					// the pipeline writes its own bookkeeping and the DDL
					// events for the table this test creates. Neither says
					// anything about the SASL connection.
					return event.Data == nil ||
						(event.Data.Schema == "pgstream" && event.Data.Table == "table_ids") ||
						event.Data.Action != "I"
				},
			}
			defer mockProcessor.close()
			startSASLKafkaReader(t, ctx, connCfg, topic, mockProcessor.process)

			testTable := "pg2kafka_sasl_" + strings.ReplaceAll(mechanism, "-", "_")
			execQuery(t, ctx, fmt.Sprintf("create table %s(id serial primary key, name text)", testTable))
			execQuery(t, ctx, fmt.Sprintf("insert into %s(name) values('a')", testTable))

			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				cancel()
				t.Fatal("timeout waiting for wal event over SASL")
			case event := <-mockProcessor.eventChan:
				require.NotNil(t, event.Data)
				require.Equal(t, "I", event.Data.Action)
				require.Equal(t, "public", event.Data.Schema)
				require.Equal(t, testTable, event.Data.Table)
			}
		})
	}

	// the mechanisms above only prove that the broker accepted the credentials
	// it was given. This proves it was the credentials that made it do so, and
	// that a rejection surfaces as an error rather than a silent stall.
	t.Run("invalid credentials", func(t *testing.T) {
		connCfg := testKafkaSASLCfg("sasl-integration-tests-rejected", "plain", testcontainers.SASLUser{
			Username: kafkaSASLUser.Username,
			Password: "not-the-password",
		})
		connCfg.Topic.AutoCreate = false

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		writer, err := kafkalib.NewWriter(kafkalib.WriterConfig{
			Conn:         connCfg,
			BatchTimeout: time.Second,
		}, log.NewNoopLogger())
		require.NoError(t, err)
		defer writer.Close()

		err = writer.WriteMessages(ctx, kafkalib.Message{Key: []byte("key"), Value: []byte("value")})
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "authentication failed")
	})
}

// ensureKafkaTopic creates the configured topic, authenticating the same way
// the pipeline does.
func ensureKafkaTopic(t *testing.T, connCfg kafkalib.ConnConfig) {
	t.Helper()

	require.True(t, connCfg.Topic.AutoCreate, "topic auto create must be on for the topic to be created")
	writer, err := kafkalib.NewWriter(kafkalib.WriterConfig{Conn: connCfg}, log.NewNoopLogger())
	require.NoError(t, err)
	require.NoError(t, writer.Close())
}

func startSASLKafkaReader(t *testing.T, ctx context.Context, connCfg kafkalib.ConnConfig, consumerGroup string, processor func(context.Context, *wal.Event) error) {
	t.Helper()

	kafkaReader, err := kafkalib.NewReader(kafkalib.ReaderConfig{
		Conn:            connCfg,
		ConsumerGroupID: consumerGroup,
	}, log.NewNoopLogger())
	require.NoError(t, err)

	reader, err := kafkalistener.NewWALReader(kafkaReader, processor)
	require.NoError(t, err)

	go func() {
		defer func() {
			reader.Close()
			kafkaReader.Close()
		}()
		reader.Listen(ctx)
	}()
}
