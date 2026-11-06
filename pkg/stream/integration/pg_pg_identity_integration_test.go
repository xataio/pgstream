// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
)

type sequenceState struct {
	lastValue int64
	isCalled  bool
}

// identitySequenceState returns the state of the sequence that backs the id
// column of the table.
func identitySequenceState(ctx context.Context, conn pglib.Querier, table string) (sequenceState, error) {
	var sequenceName string
	if err := conn.QueryRow(ctx, []any{&sequenceName}, "SELECT pg_get_serial_sequence($1, 'id')", table); err != nil {
		return sequenceState{}, fmt.Errorf("resolving the sequence of %s.id: %w", table, err)
	}

	var state sequenceState
	err := conn.QueryRow(ctx, []any{&state.lastValue, &state.isCalled},
		fmt.Sprintf("SELECT last_value, is_called FROM %s", sequenceName))
	if err != nil {
		return sequenceState{}, fmt.Errorf("reading sequence %s: %w", sequenceName, err)
	}
	return state, nil
}

// Test_PostgresToPostgres_AlwaysIdentityOnConflictUpdate verifies that INSERT
// events on a table with a GENERATED ALWAYS AS IDENTITY column are replicated
// when the writer uses the "update" on conflict action.
//
// The upsert must not assign the identity column in the ON CONFLICT SET list:
// postgres rejects it with `column "id" can only be updated to DEFAULT`, and
// OVERRIDING SYSTEM VALUE does not apply there.
func Test_PostgresToPostgres_AlwaysIdentityOnConflictUpdate(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(),
	}
	cfg.Processor.Postgres.BatchWriter.OnConflictAction = "update"
	// the injector provides the primary key columns. Without them the writer
	// has no conflict target and builds the insert without ON CONFLICT.
	cfg.Processor.Injector = &injector.Config{URL: pgurl}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	runStream(t, ctx, cfg)

	testTable := fmt.Sprintf("pg2pg_always_identity_upsert_%d", time.Now().UnixNano())
	defer execQuery(t, ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))

	execQuery(t, ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name text
		)
	`, testTable))

	require.Eventually(t, func() bool {
		return len(getInformationSchemaColumns(t, ctx, targetConn, testTable)) == 2
	}, 20*time.Second, 200*time.Millisecond, "table schema not replicated")

	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(name) VALUES('Alice')", testTable))

	require.Eventually(t, func() bool {
		rows := getIDNameRows(t, ctx, targetConn, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", testTable))
		return len(rows) == 1 && rows[0] == idNameRow{id: 1, name: "Alice"}
	}, 20*time.Second, 200*time.Millisecond,
		"INSERT not replicated: the upsert likely assigned the always-identity column in ON CONFLICT DO UPDATE SET")
}
