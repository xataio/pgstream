// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	natslib "github.com/xataio/pgstream/pkg/nats"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal"
	natsjslistener "github.com/xataio/pgstream/pkg/wal/listener/natsjetstream"
)

func Test_PostgresToNATSJetstream(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(),
		Processor: testNATSJetstreamProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	// use a mock processor and a NATS JetStream reader to validate messages
	// are properly sent to the stream
	mockProcessor := &mockProcessor{
		eventChan: make(chan *wal.Event),
		skipEventFn: func(event *wal.Event) bool {
			return event.Data != nil && event.Data.Schema == "pgstream" && event.Data.Table == "table_ids"
		},
	}
	defer mockProcessor.close()
	startNATSJetstreamReader(t, ctx, mockProcessor.process)

	testTable := "pg2natsjetstream_integration_test"

	testDDLEvent := &wal.DDLEvent{
		SchemaName: "public",
		CommandTag: "CREATE TABLE",
		DDL:        "create table pg2natsjetstream_integration_test(id serial primary key, name text)",
		Objects: []wal.DDLObject{
			{
				Type:     "table",
				Identity: "public.pg2natsjetstream_integration_test",
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
						Default:   ptr("nextval('public.pg2natsjetstream_integration_test_id_seq'::regclass)"),
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
				Identity: "public.pg2natsjetstream_integration_test_pkey",
				Schema:   "public",
			},
			{
				Type:     "sequence",
				Identity: "public.pg2natsjetstream_integration_test_id_seq",
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

func startNATSJetstreamReader(t *testing.T, ctx context.Context, processor func(context.Context, *wal.Event) error) {
	listenerCfg := testNATSJetstreamListenerCfg()
	natsReader, err := natslib.NewReader(listenerCfg.NATSJetstream.Reader, log.NewNoopLogger())
	require.NoError(t, err)
	reader, err := natsjslistener.NewWALReader(natsReader, processor)
	require.NoError(t, err)
	go func() {
		defer func() {
			reader.Close()
			natsReader.Close()
		}()
		reader.Listen(ctx)
	}()
}
