// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	batchmocks "github.com/xataio/pgstream/pkg/wal/processor/batch/mocks"
)

// TestBulkIngestWriter_DDLEvent_KafkaPath reproduces the root cause of missing
// FK constraints in the Kafka replication path:
//
// In Kafka mode, snapshot DDL (CREATE TABLE, ALTER TABLE ADD CONSTRAINT,
// CREATE INDEX) arrives through the same Kafka topic as data rows. The
// BulkIngestWriter previously:
//  1. Set config.IgnoreDDL = true → DDL adapter was nil
//  2. Rejected all non-insert events at line 83: if !walEvent.Data.IsInsert()
//
// This silently dropped every DDL event. FK constraints, PKs, and indexes
// from the snapshot post-data phase never reached the target database.
//
// The fix removes IgnoreDDL=true and adds processDDLEvent which flushes
// pending COPY batches then executes DDL directly.
func TestBulkIngestWriter_DDLEvent_KafkaPath(t *testing.T) {
	t.Parallel()

	ddlSQL := `ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id)`

	// Build a DDL WAL event as restoreToWAL would create it
	ddlContent := `{"ddl":"` + ddlSQL + `","schema_name":"public","command_tag":"ALTER TABLE","objects":[]}`
	ddlEvent := &wal.Event{
		Data: &wal.Data{
			Action:    wal.LogicalMessageAction,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			LSN:       wal.ZeroLSN,
			Prefix:    wal.DDLPrefix,
			Content:   ddlContent,
		},
	}

	// Verify this IS a DDL event (the check the old code never reached)
	require.True(t, ddlEvent.Data.IsDDLEvent(), "test event must be recognized as DDL")

	// Track what SQL the mock executes
	var executedSQL []string
	var mu sync.Mutex

	mockPgConn := &pgmocks.Querier{
		ExecFn: func(ctx context.Context, callNum uint, sql string, args ...any) (pglib.CommandTag, error) {
			mu.Lock()
			executedSQL = append(executedSQL, sql)
			mu.Unlock()
			return pglib.CommandTag{}, nil
		},
	}

	// The adapter must produce a DDL query from the event. Use the real
	// ddlAdapter (not a mock) to verify the full code path.
	ddlQueryAdapter := newDDLAdapter()
	realAdapter := &adapter{
		ddlAdapter:      ddlQueryAdapter,
		ddlEventAdapter: wal.WalDataToDDLEvent,
		schemaObserver: &mockSchemaObserver{
			isMaterializedViewFn: func(schema, table string) bool { return false },
			updateFn:             func(ddlEvent *wal.DDLEvent) {},
		},
	}

	writer := &BulkIngestWriter{
		Writer: &Writer{
			logger: loglib.NewNoopLogger(),
			pgConn: mockPgConn,
			adapter: realAdapter,
		},
		batchSenderMap:     synclib.NewMap[string, queryBatchSender](),
		batchSenderBuilder: func(ctx context.Context, schema, table string) (queryBatchSender, error) {
			return batchmocks.NewBatchSender[*query](), nil
		},
	}

	err := writer.ProcessWALEvent(context.Background(), ddlEvent)
	require.NoError(t, err)

	// The DDL must have been executed, not dropped
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, executedSQL, 1, "DDL event must result in exactly one Exec call")
	require.Equal(t, ddlSQL, executedSQL[0], "executed SQL must match the DDL statement")
}

// TestBulkIngestWriter_DDLEvent_FlushesBeforeDDL verifies that pending COPY
// batches are flushed before DDL execution. Post-data DDL (ALTER TABLE ADD
// CONSTRAINT) validates existing rows — if data is still buffered in batch
// senders, the constraint creation fails on missing rows.
func TestBulkIngestWriter_DDLEvent_FlushesBeforeDDL(t *testing.T) {
	t.Parallel()

	ddlSQL := `ALTER TABLE ONLY public.users ADD CONSTRAINT users_pkey PRIMARY KEY (id)`
	ddlContent := `{"ddl":"` + ddlSQL + `","schema_name":"public","command_tag":"ALTER TABLE","objects":[]}`
	ddlEvent := &wal.Event{
		Data: &wal.Data{
			Action:    wal.LogicalMessageAction,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			LSN:       wal.ZeroLSN,
			Prefix:    wal.DDLPrefix,
			Content:   ddlContent,
		},
	}

	// Track execution order: batch close must happen before DDL exec
	var orderLog []string
	var mu sync.Mutex
	appendLog := func(entry string) {
		mu.Lock()
		orderLog = append(orderLog, entry)
		mu.Unlock()
	}

	mockPgConn := &pgmocks.Querier{
		ExecFn: func(ctx context.Context, callNum uint, sql string, args ...any) (pglib.CommandTag, error) {
			appendLog("exec:" + sql)
			return pglib.CommandTag{}, nil
		},
	}

	// Create a mock batch sender that logs when Close is called
	mockSender := batchmocks.NewBatchSender[*query]()
	originalClose := mockSender.CloseFn
	mockSender.CloseFn = func() {
		appendLog("batch_close")
		if originalClose != nil {
			originalClose()
		}
	}

	ddlQueryAdapter := newDDLAdapter()
	realAdapter := &adapter{
		ddlAdapter:      ddlQueryAdapter,
		ddlEventAdapter: wal.WalDataToDDLEvent,
		schemaObserver: &mockSchemaObserver{
			isMaterializedViewFn: func(schema, table string) bool { return false },
			updateFn:             func(ddlEvent *wal.DDLEvent) {},
		},
	}

	// Pre-populate a batch sender to simulate pending data
	senderMap := synclib.NewMap[string, queryBatchSender]()
	senderMap.Set(pglib.QuoteQualifiedIdentifier("public", "users"), mockSender)

	writer := &BulkIngestWriter{
		Writer: &Writer{
			logger: loglib.NewNoopLogger(),
			pgConn: mockPgConn,
			adapter: realAdapter,
		},
		batchSenderMap: senderMap,
		batchSenderBuilder: func(ctx context.Context, schema, table string) (queryBatchSender, error) {
			return batchmocks.NewBatchSender[*query](), nil
		},
	}

	err := writer.ProcessWALEvent(context.Background(), ddlEvent)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	// Batch must be closed BEFORE DDL executes
	require.Len(t, orderLog, 2, "expected batch_close then exec")
	require.Equal(t, "batch_close", orderLog[0], "pending batches must flush before DDL")
	require.Equal(t, "exec:"+ddlSQL, orderLog[1], "DDL must execute after flush")
}

// TestBulkIngestWriter_DDLEvent_InsertStillWorks verifies that INSERT events
// continue to work normally after the DDL handling code was added.
func TestBulkIngestWriter_DDLEvent_InsertStillWorks(t *testing.T) {
	t.Parallel()

	insertEvent := &wal.Event{
		Data: &wal.Data{
			Action: "I",
			LSN:    testLSNStr,
			Schema: "public",
			Table:  "users",
		},
		CommitPosition: testCommitPosition,
	}

	insertQuery := &query{
		schema:      "public",
		table:       "users",
		columnNames: []string{"id", "name"},
		sql:         "INSERT INTO users(id, name) VALUES($1, $2)",
		args:        []any{1, "alice"},
	}

	mockAdapter := &mockAdapter{
		walEventToQueriesFn: func(e *wal.Event) ([]*query, error) {
			return []*query{insertQuery}, nil
		},
	}

	mockSender := batchmocks.NewBatchSender[*query]()
	senderMap := synclib.NewMap[string, queryBatchSender]()
	senderMap.Set(pglib.QuoteQualifiedIdentifier("public", "users"), mockSender)

	writer := &BulkIngestWriter{
		Writer: &Writer{
			logger:  loglib.NewNoopLogger(),
			adapter: mockAdapter,
		},
		batchSenderMap: senderMap,
		batchSenderBuilder: func(ctx context.Context, schema, table string) (queryBatchSender, error) {
			return nil, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		defer mockSender.Close()
		err := writer.ProcessWALEvent(ctx, insertEvent)
		require.NoError(t, err)
	}()

	msgs := mockSender.GetWALMessages()
	require.Len(t, msgs, 1, "INSERT event must still be batched normally")
}
