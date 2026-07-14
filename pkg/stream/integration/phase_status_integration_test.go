// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/phase"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/stream"
)

func Test_PhaseStatus_SnapshotAndReplication(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTable := "phase_status_integration_test"
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("create table %s(id serial primary key, name text)", testTable))
	// bulk rows lengthen the snapshot window so /status can observe phase=snapshot
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf(
		"insert into %s(name) select 'row_' || g from generate_series(1, 5000) g", testTable))

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfgWithSnapshot(snapshotPGURL, targetPGURL, []string{testTable}),
		Processor: testPostgresProcessorCfg(),
	}
	initStream(t, ctx, snapshotPGURL)

	tracker := phase.NewTracker()
	statusURL, stopHealth := startPhaseHealthServer(t, tracker)
	defer stopHealth()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	done := make(chan error, 1)
	go func() {
		done <- stream.Run(runCtx, testLogger(), cfg, false, nil, tracker)
	}()

	var sawSnapshot, sawReplication bool
	require.Eventually(t, func() bool {
		switch fetchStatusPhase(t, statusURL) {
		case string(phase.Snapshot):
			sawSnapshot = true
		case string(phase.Replication):
			sawReplication = true
		}
		return sawReplication
	}, 90*time.Second, 25*time.Millisecond, "timed out waiting for /status to report replication phase")

	require.True(t, sawSnapshot, "expected /status to report snapshot phase during initial snapshot")

	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("insert into %s(name) values('live')", testTable))

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rows, err := targetConn.Query(ctx, fmt.Sprintf("select name from %s where name = 'live'", testTable))
		if err != nil {
			return false
		}
		defer rows.Close()
		return rows.Next()
	}, 20*time.Second, time.Second, "replication did not deliver post-snapshot insert to target")

	runCancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for stream to stop")
	}
}
