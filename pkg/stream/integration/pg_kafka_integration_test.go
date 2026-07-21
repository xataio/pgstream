// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal"
	kafkalistener "github.com/xataio/pgstream/pkg/wal/listener/kafka"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
)

func Test_PostgresToKafka(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testKafkaProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	// use a mock processor and a kafka reader to validate the kafka messages
	// are properly sent to the topic
	mockProcessor := &mockProcessor{
		eventChan: make(chan *wal.Event),
		skipEventFn: func(event *wal.Event) bool {
			return event.Data != nil && event.Data.Schema == "pgstream" && event.Data.Table == "table_ids"
		},
	}
	defer mockProcessor.close()
	startKafkaReader(t, ctx, mockProcessor.process)

	testTable := "pg2kafka_integration_test"

	testDDLEvent := &wal.DDLEvent{
		SchemaName: "public",
		CommandTag: "CREATE TABLE",
		DDL:        "create table pg2kafka_integration_test(id serial primary key, name text)",
		Objects: []wal.DDLObject{
			{
				Type:     "table",
				Identity: "public.pg2kafka_integration_test",
				Schema:   "public",
				Columns: []wal.DDLColumn{
					{
						Name:      "id",
						Attnum:    1,
						Type:      "integer",
						Nullable:  false,
						Generated: false,
						Unique:    true,
						Identity:  nil,
						Default:   ptr("nextval('public.pg2kafka_integration_test_id_seq'::regclass)"),
					},
					{
						Name:      "name",
						Attnum:    2,
						Type:      "text",
						Nullable:  true,
						Generated: false,
						Unique:    false,
						Identity:  nil,
						Default:   nil,
					},
				},
				PrimaryKeyColumns: []string{"id"},
			},
			{
				Type:     "index",
				Identity: "public.pg2kafka_integration_test_pkey",
				Schema:   "public",
			},
			{
				Type:     "sequence",
				Identity: "public.pg2kafka_integration_test_id_seq",
				Schema:   "public",
			},
		},
	}

	ddlEventBytes, err := json.Marshal(testDDLEvent)
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string

		wantEvent *wal.Event
	}{
		{
			name:  "ddl event",
			query: fmt.Sprintf("create table %s(id serial primary key, name text)", testTable),

			wantEvent: &wal.Event{
				Data: &wal.Data{
					Action:  wal.LogicalMessageAction,
					Prefix:  wal.DDLPrefix,
					Content: string(ddlEventBytes),
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
					require.Equal(t, tc.wantEvent.Data.Prefix, event.Data.Prefix)
					if event.Data.Content != "" && tc.wantEvent.Data.Content != "" {
						ddlEvent := &wal.DDLEvent{}
						require.NoError(t, json.Unmarshal([]byte(event.Data.Content), ddlEvent))
						require.Empty(t, cmp.Diff(ddlEvent, testDDLEvent,
							cmpopts.IgnoreFields(wal.DDLObject{}, "OID", "PgstreamID"),
							cmpopts.SortSlices(func(a, b wal.DDLObject) bool { return a.Type < b.Type })))
					}
					return
				}
			}
		})
	}
}

func Test_PostgresToKafka_PartitionKeyPrimaryKey(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	// use a dedicated topic to prevent cross talk with other tests sharing the
	// kafka container
	const topic = "integration-tests-partition-key"
	processorCfg := testKafkaProcessorCfg()
	processorCfg.Kafka.Writer.Kafka.Topic.Name = topic
	processorCfg.Kafka.Writer.PartitionKey = kafkaprocessor.PartitionKeyPrimaryKey

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: processorCfg,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	// the batch writer creates the topic on startup, but its metadata can take
	// a while to propagate on a busy broker, causing the first write to fail
	// with an unknown topic error. Wait for the topic to be visible before
	// generating any wal events.
	waitForKafkaTopic(t, topic)

	testTable := "pg2kafka_partition_key_test"
	execQuery(t, ctx, fmt.Sprintf("create table %s(id serial primary key, name text)", testTable))
	execQuery(t, ctx, fmt.Sprintf("insert into %s(name) values('a')", testTable))
	execQuery(t, ctx, fmt.Sprintf("update %s set name = 'b' where id = 1", testTable))
	execQuery(t, ctx, fmt.Sprintf("delete from %s where id = 1", testTable))

	// use a raw kafka reader to validate the message keys on the wire
	kafkaReader, err := kafka.NewReader(kafka.ReaderConfig{
		Conn: kafka.ConnConfig{
			Servers: kafkaBrokers,
			Topic: kafka.TopicConfig{
				Name:       topic,
				AutoCreate: true,
			},
		},
		ConsumerGroupID: "partition-key-test-group",
	}, log.NewNoopLogger())
	require.NoError(t, err)
	defer kafkaReader.Close()

	rowKey := fmt.Sprintf("public.%s:1", testTable)
	wantKeys := map[string]string{
		"ddl": "public",
		"I":   rowKey,
		"U":   rowKey,
		"D":   rowKey,
	}

	gotKeys := map[string]string{}
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Minute)
	defer timeoutCancel()
	for len(gotKeys) < len(wantKeys) {
		msg, err := kafkaReader.FetchMessage(timeoutCtx)
		require.NoError(t, err, "waiting for kafka messages, got keys: %v", gotKeys)
		if len(msg.Value) == 0 {
			continue
		}
		data := &wal.Data{}
		require.NoError(t, json.Unmarshal(msg.Value, data))
		switch {
		case data.IsDDLEvent():
			// DDL events must keep the schema key regardless of the configured
			// partition key strategy
			if strings.Contains(data.Content, testTable) {
				gotKeys["ddl"] = string(msg.Key)
			}
		case data.Schema == "public" && data.Table == testTable:
			gotKeys[data.Action] = string(msg.Key)
		}
	}

	require.Equal(t, wantKeys, gotKeys)
}

func waitForKafkaTopic(t *testing.T, topic string) {
	t.Helper()

	require.Eventually(t, func() bool {
		conn, err := kafkago.Dial("tcp", kafkaBrokers[0])
		if err != nil {
			return false
		}
		defer conn.Close()
		partitions, err := conn.ReadPartitions(topic)
		return err == nil && len(partitions) > 0
	}, 30*time.Second, 500*time.Millisecond, "timeout waiting for kafka topic %s to be created", topic)
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

func ptr[T any](i T) *T { return &i }
