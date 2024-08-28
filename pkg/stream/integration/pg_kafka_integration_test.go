// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
)

func Test_PostgresToKafka(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(),
		Processor: testKafkaProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	// use a mock processor and a kafka reader to validate the kafka messages
	// are properly sent to the topic
	mockProcessor := &mockProcessor{
		eventChan: make(chan *wal.Event),
	}
	defer mockProcessor.close()
	startKafkaReader(t, ctx, mockProcessor.process)

	testTable := "pg2kafka_integration_test"

	tests := []struct {
		name  string
		query string

		wantEvent *wal.Event
	}{
		{
			name:  "schema event",
			query: fmt.Sprintf("create table %s(id serial primary key, name text)", testTable),

			wantEvent: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Schema: schemalog.SchemaName,
					Table:  schemalog.TableName,
				},
			},
		},
		{
			name:  "data event",
			query: fmt.Sprintf("insert into %s(name) values('a')", testTable),

			wantEvent: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Schema: "public",
					Table:  testTable,
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			execQuery(t, ctx, tc.query)

			timer := time.NewTimer(20 * time.Second)
			defer timer.Stop()
			for {
				select {
				case <-timer.C:
					cancel()
					t.Error("timeout waiting for wal event")
					return
				case event := <-mockProcessor.eventChan:
					require.NotNil(t, event.Data)
					require.Equal(t, tc.wantEvent.Data.Action, event.Data.Action)
					require.Equal(t, tc.wantEvent.Data.Schema, event.Data.Schema)
					require.Equal(t, tc.wantEvent.Data.Table, event.Data.Table)
					return
				}
			}
		})
	}
}

func startKafkaReader(t *testing.T, ctx context.Context, processor func(context.Context, *wal.Event) error) {
	kafkaReader, err := kafka.NewReader(testKafkaListenerCfg().Kafka.Reader, log.NewNoopLogger())
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
