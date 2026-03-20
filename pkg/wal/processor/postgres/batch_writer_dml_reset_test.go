// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

// TestBatchWriter_sendBatch_DMLNotReExecutedAfterDDL reproduces duplicate row
// insertion caused by dmlQueries not being reset after flush in sendBatch.
//
// When a batch contains [INSERT_1, INSERT_2, DDL_pk, DDL_fk], the unfixed code
// flushes dmlQueries before each DDL AND at the end — but never clears the
// slice. Each flush re-executes all previously flushed INSERTs.
func TestBatchWriter_sendBatch_DMLNotReExecutedAfterDDL(t *testing.T) {
	t.Parallel()

	insert1 := &query{
		schema: "public", table: "jsonb_table",
		sql: `INSERT INTO "public"."jsonb_table"("id", "data") VALUES($1, $2)`,
		args: []any{1, `{"key":"value_1"}`},
	}
	insert2 := &query{
		schema: "public", table: "jsonb_table",
		sql: `INSERT INTO "public"."jsonb_table"("id", "data") VALUES($1, $2)`,
		args: []any{2, `{"key":"value_2"}`},
	}
	ddlPK := &query{
		schema: "public", table: "jsonb_table",
		sql:   `ALTER TABLE ONLY public.jsonb_table ADD CONSTRAINT jsonb_table_pkey PRIMARY KEY (id);`,
		isDDL: true,
	}
	ddlFK := &query{
		schema: "public", table: "child_table",
		sql:   `ALTER TABLE ONLY public.child_table ADD CONSTRAINT child_table_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.parent_table(id);`,
		isDDL: true,
	}

	b := batch.NewBatch([]*query{insert1, insert2, ddlPK, ddlFK}, nil)

	var execLog []string
	var mu sync.Mutex
	appendLog := func(entry string) {
		mu.Lock()
		execLog = append(execLog, entry)
		mu.Unlock()
	}

	mockPgConn := &pgmocks.Querier{
		ExecInTxFn: func(ctx context.Context, fn func(tx pglib.Tx) error) error {
			tx := &pgmocks.Tx{
				ExecFn: func(ctx context.Context, callNum uint, sql string, args ...any) (pglib.CommandTag, error) {
					appendLog("tx:" + sql)
					return pglib.CommandTag{}, nil
				},
			}
			return fn(tx)
		},
		ExecFn: func(ctx context.Context, callNum uint, sql string, args ...any) (pglib.CommandTag, error) {
			appendLog("exec:" + sql)
			return pglib.CommandTag{}, nil
		},
	}

	writer := &BatchWriter{
		Writer: &Writer{
			logger: loglib.NewNoopLogger(),
			pgConn: mockPgConn,
		},
	}

	err := writer.sendBatch(context.Background(), b)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	t.Logf("Full exec log (%d entries): %v", len(execLog), execLog)

	// Count DML and DDL executions
	dmlCount := 0
	ddlCount := 0
	for _, entry := range execLog {
		switch {
		case entry == "tx:"+insert1.sql:
			dmlCount++
		case entry == "exec:"+ddlPK.sql || entry == "exec:"+ddlFK.sql:
			ddlCount++
		}
	}

	// 2 INSERT statements must execute exactly 2 times total (once each).
	// Before the fix, dmlQueries was not reset after DDL flush, so each
	// subsequent DDL event re-flushed all previously executed INSERTs.
	// With [INSERT_1, INSERT_2, DDL_pk, DDL_fk], the bug caused:
	//   flush before DDL_pk: INSERT_1 + INSERT_2 (2 executions)
	//   flush before DDL_fk: INSERT_1 + INSERT_2 again (4 total)
	//   flush at end: INSERT_1 + INSERT_2 again (6 total)
	require.Equal(t, 2, dmlCount,
		"2 INSERTs must execute exactly 2 times total (once each), got %d (dmlQueries re-executed after flush)", dmlCount)
	require.Equal(t, 2, ddlCount, "both DDL statements must execute")
}
