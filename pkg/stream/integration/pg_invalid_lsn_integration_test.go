// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/stream"
)

// Test_PostgresToPostgres_RestartDoesNotSkipWALAfterLargeRow reproduces the
// invalid-LSN failure end to end:
//
//  1. replicate a row whose wal2json representation is much larger than the
//     WAL distance from the row change to its transaction commit;
//  2. stop pgstream after that row is durable on the target;
//  3. start a fresh pgstream instance on the same replication slot;
//  4. insert another row and require it to reach the target.
//
// The broken implementation adds len(WALData) to XLogData.WALStart. The first
// row therefore moves confirmed_flush_lsn beyond PostgreSQL's current WAL. On
// restart, the second transaction is behind that fabricated start position and
// is skipped. The large value uses JSON-escaped control characters so its JSON
// is large while its toasted PostgreSQL representation remains small.
func Test_PostgresToPostgres_RestartDoesNotSkipWALAfterLargeRow(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	testCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tableName := fmt.Sprintf("pg2pg_invalid_lsn_%d", time.Now().UnixNano())
	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(),
	}
	slotName := cfg.Listener.Postgres.Replication.ReplicationSlotName

	sourceConn, err := pglib.NewConn(testCtx, pgurl)
	require.NoError(t, err)
	defer sourceConn.Close(context.Background())

	targetConn, err := pglib.NewConn(testCtx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(context.Background())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		execQueryWithURL(t, cleanupCtx, pgurl, "DROP TABLE IF EXISTS "+tableName)
		execQueryWithURL(t, cleanupCtx, targetPGURL, "DROP TABLE IF EXISTS "+tableName)
	})

	stopFirstStream := startRestartableStream(t, cfg)

	_, err = sourceConn.Exec(testCtx, fmt.Sprintf(`
		CREATE TABLE %s (
			id bigint PRIMARY KEY,
			payload text NOT NULL
		)`, tableName))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exists, err := tableExists(testCtx, targetConn, "public", tableName)
		return err == nil && exists
	}, 20*time.Second, 200*time.Millisecond, "expected the test table to be replicated")

	// Each byte becomes the six-byte JSON escape "\\u0001" in wal2json. The
	// repeated source value is also highly compressible by PostgreSQL TOAST.
	largePayload := strings.Repeat("\x01", 1<<20)
	_, err = sourceConn.Exec(testCtx,
		fmt.Sprintf("INSERT INTO %s (id, payload) VALUES ($1, $2)", tableName),
		1, largePayload)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var payloadLength int
		err := targetConn.QueryRow(testCtx, []any{&payloadLength},
			fmt.Sprintf("SELECT octet_length(payload) FROM %s WHERE id = 1", tableName))
		return err == nil && payloadLength == len(largePayload)
	}, 30*time.Second, 200*time.Millisecond, "expected the large first row to be replicated")

	// Give the batch checkpointer a chance to report the first row. On the
	// broken implementation this returns as soon as confirmed_flush_lsn moves
	// ahead of pg_current_wal_lsn. On a corrected implementation it simply
	// times out and returns the latest, valid positions.
	confirmedLSN, currentLSN, aheadBytes := observeAheadCheckpoint(testCtx, sourceConn, slotName, 5*time.Second)
	t.Logf("before restart: confirmed_flush_lsn=%s, current_wal_lsn=%s, ahead_bytes=%d",
		confirmedLSN, currentLSN, aheadBytes)

	stopFirstStream()

	// This creates a new listener/handler/replication connection. It reads the
	// slot's confirmed_flush_lsn again, which is the restart path under test.
	stopSecondStream := startRestartableStream(t, cfg)
	defer stopSecondStream()

	_, err = sourceConn.Exec(testCtx,
		fmt.Sprintf("INSERT INTO %s (id, payload) VALUES ($1, $2)", tableName),
		2, "second-row")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var payload string
		err := targetConn.QueryRow(testCtx, []any{&payload},
			fmt.Sprintf("SELECT payload FROM %s WHERE id = 2", tableName))
		return err == nil && payload == "second-row"
	}, 20*time.Second, 200*time.Millisecond,
		"the second row was skipped after pgstream restarted from an invalid confirmed_flush_lsn")
}

func startRestartableStream(t *testing.T, cfg *stream.Config) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- stream.Run(ctx, loglib.NewNoopLogger(), cfg, false, nil)
	}()

	var once sync.Once
	stop := func() {
		t.Helper()
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for pgstream to stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func observeAheadCheckpoint(
	ctx context.Context,
	conn pglib.Querier,
	slotName string,
	maxWait time.Duration,
) (confirmedLSN string, currentLSN string, aheadBytes int64) {
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		err := conn.QueryRow(ctx, []any{&confirmedLSN, &currentLSN, &aheadBytes}, `
			SELECT
				confirmed_flush_lsn::text,
				pg_current_wal_lsn()::text,
				pg_wal_lsn_diff(confirmed_flush_lsn, pg_current_wal_lsn())::bigint
			FROM pg_replication_slots
			WHERE slot_name = $1`, slotName)
		if err == nil && aheadBytes > 0 {
			return confirmedLSN, currentLSN, aheadBytes
		}

		select {
		case <-ctx.Done():
			return confirmedLSN, currentLSN, aheadBytes
		case <-timer.C:
			return confirmedLSN, currentLSN, aheadBytes
		case <-ticker.C:
		}
	}
}
