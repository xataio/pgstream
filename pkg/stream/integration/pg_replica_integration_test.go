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
	"github.com/xataio/pgstream/internal/testcontainers"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

// Test_ReplicaSourceSnapshotAndReplication runs the whole pipeline — snapshot
// and replication — against a physical standby rather than the primary, which
// is how a deployment keeps snapshot load off a primary that cannot afford it.
//
// The arrangement rests on one property that is worth pinning down with a test,
// because it is not obvious: pgstream's DDL replication keeps working even
// though its event trigger can only ever run on the primary. The trigger emits
// DDL through pg_logical_emit_message, which writes an ordinary WAL record; the
// standby replays that record like any other, so a logical slot on the standby
// decodes it. Nothing has to be (or can be) installed on the standby itself.
func Test_ReplicaSourceSnapshotAndReplication(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	var primaryURL, replicaURL string
	pgcleanup, err := testcontainers.SetupPostgresPrimaryReplica(context.Background(), &primaryURL, &replicaURL)
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const testTable = "replica_source_integration_test"

	// pgstream's internal state goes on the primary. The standby is read only,
	// and an event trigger there would never fire anyway: WAL replay is
	// physical redo, not command execution. MigrationsOnly skips creating a
	// slot on the primary, since the slot this test uses lives on the standby.
	require.NoError(t, stream.Init(ctx, &stream.InitConfig{
		PostgresURL:    primaryURL,
		MigrationsOnly: true,
	}))

	// seed on the primary before the snapshot, so the snapshot has pre-existing
	// rows to copy rather than picking them up through replication
	execQueryWithURL(t, ctx, primaryURL, fmt.Sprintf(
		`CREATE TABLE %s(id serial PRIMARY KEY, name TEXT)`, testTable))
	execQueryWithURL(t, ctx, primaryURL, fmt.Sprintf(
		`INSERT INTO %s(name) VALUES('a'),('b')`, testTable))

	waitForStandbyReplay(t, ctx, primaryURL, replicaURL)

	slotName := createLogicalSlotOnStandby(t, ctx, primaryURL, replicaURL)

	cfg := &stream.Config{
		Listener:  replicaListenerCfg(replicaURL, targetPGURL, slotName, []string{testTable}),
		Processor: testPostgresProcessorCfgWithTargetURL(targetPGURL),
	}
	runStream(t, ctx, cfg)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	// the snapshot reads from the standby, including the parallel path that
	// exports a transaction snapshot and imports it on sibling connections
	require.Eventually(t, func() bool {
		rows := fetchReplicaTestRows(t, ctx, targetConn, testTable, false)
		return len(rows) == 2
	}, 60*time.Second, time.Second, "timeout waiting for snapshot from standby")

	require.ElementsMatch(t,
		[]replicaTestRow{{id: 1, name: "a"}, {id: 2, name: "b"}},
		fetchReplicaTestRows(t, ctx, targetConn, testTable, false))

	// DDL runs on the primary, so the event trigger fires there. If the emitted
	// logical message survives the trip through physical replication and out of
	// the standby's logical slot, the new column shows up on the target.
	execQueryWithURL(t, ctx, primaryURL, fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN email TEXT`, testTable))
	execQueryWithURL(t, ctx, primaryURL, fmt.Sprintf(
		`INSERT INTO %s(name, email) VALUES('c','c@test.com')`, testTable))

	require.Eventually(t, func() bool {
		if !targetHasColumn(t, ctx, targetConn, testTable, "email") {
			return false
		}
		return len(fetchReplicaTestRows(t, ctx, targetConn, testTable, true)) == 3
	}, 60*time.Second, time.Second, "timeout waiting for DDL and DML replicated from standby")

	require.ElementsMatch(t, []replicaTestRow{
		{id: 1, name: "a"},
		{id: 2, name: "b"},
		{id: 3, name: "c", email: "c@test.com"},
	}, fetchReplicaTestRows(t, ctx, targetConn, testTable, true))

	// A logical slot on a standby is invalidated by recovery conflicts unless
	// hot_standby_feedback keeps the primary from vacuuming rows the decoder
	// still needs. Without this check the test would pass simply by being fast,
	// and hide a setup that falls over on a busier primary.
	requireSlotNotConflicting(t, ctx, replicaURL, slotName)
}

func replicaListenerCfg(sourceURL, targetURL, slotName string, tables []string) stream.ListenerConfig {
	return stream.ListenerConfig{
		Postgres: &stream.PostgresListenerConfig{
			URL: sourceURL,
			Replication: pgreplication.Config{
				PostgresURL:         sourceURL,
				ReplicationSlotName: slotName,
			},
			Snapshot: &snapshotbuilder.SnapshotListenerConfig{
				Data: &pgsnapshotgenerator.Config{
					URL: sourceURL,
					// exercise the parallel path, so the test covers
					// SET TRANSACTION SNAPSHOT against a standby rather than
					// quietly passing through a single connection
					SchemaWorkers: 2,
					TableWorkers:  2,
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

// waitForStandbyReplay blocks until the standby has replayed everything the
// primary had written when it was called. Any assertion that follows a write on
// the primary needs this, otherwise it races replication.
func waitForStandbyReplay(t *testing.T, ctx context.Context, primaryURL, replicaURL string) {
	t.Helper()

	primaryConn, err := pglib.NewConn(ctx, primaryURL)
	require.NoError(t, err)
	defer primaryConn.Close(ctx)

	var primaryLSN string
	require.NoError(t, primaryConn.QueryRow(ctx, []any{&primaryLSN}, "SELECT pg_current_wal_lsn()::text"))

	replicaConn, err := pglib.NewConn(ctx, replicaURL)
	require.NoError(t, err)
	defer replicaConn.Close(ctx)

	require.Eventually(t, func() bool {
		var replayed bool
		if err := replicaConn.QueryRow(ctx, []any{&replayed},
			"SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn", primaryLSN); err != nil {
			return false
		}
		return replayed
	}, 60*time.Second, 200*time.Millisecond, "standby did not replay up to %s", primaryLSN)
}

// createLogicalSlotOnStandby creates the slot pgstream will stream from, on the
// standby rather than the primary.
//
// Creating a logical slot on a standby blocks until the primary emits an
// xl_running_xacts record, which is what lets the standby build a consistent
// catalog snapshot. A quiet primary may never emit one on its own, so this
// nudges it with pg_log_standby_snapshot() until the slot appears. Skipping
// that turns an idle primary into an indefinite hang rather than a failure.
func createLogicalSlotOnStandby(t *testing.T, ctx context.Context, primaryURL, replicaURL string) string {
	t.Helper()

	slotName := fmt.Sprintf("pgstream_replica_it_%d", time.Now().UnixNano())

	// slot-only init is what makes this workable against a standby: it creates
	// the slot without attempting the pgstream schema or migrations, which
	// would fail on a read only server
	created := make(chan error, 1)
	go func() {
		created <- stream.Init(ctx, &stream.InitConfig{
			PostgresURL:         replicaURL,
			ReplicationSlotName: slotName,
			SlotOnly:            true,
		})
	}()

	primaryConn, err := pglib.NewConn(ctx, primaryURL)
	require.NoError(t, err)
	defer primaryConn.Close(ctx)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(90 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case err := <-created:
			// no cleanup registered for the slot: it lives on the standby
			// container, which this test owns and tears down on the way out. A
			// t.Cleanup here would run after the containers are already gone.
			require.NoError(t, err, "creating logical replication slot on standby")
			return slotName
		case <-ticker.C:
			// best effort: the slot creation is the thing being waited on, and
			// a failed nudge just means the next tick tries again
			_, _ = primaryConn.Exec(ctx, "SELECT pg_log_standby_snapshot()")
		case <-timeout.C:
			t.Fatal("timeout creating logical replication slot on standby")
			return ""
		}
	}
}

func requireSlotNotConflicting(t *testing.T, ctx context.Context, replicaURL, slotName string) {
	t.Helper()

	conn, err := pglib.NewConn(ctx, replicaURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	var conflicting *bool
	require.NoError(t, conn.QueryRow(ctx, []any{&conflicting},
		"SELECT conflicting FROM pg_replication_slots WHERE slot_name = $1", slotName),
		"logical slot %s is gone from the standby", slotName)

	if conflicting != nil {
		require.False(t, *conflicting,
			"logical slot %s was invalidated by a recovery conflict on the standby", slotName)
	}
}

type replicaTestRow struct {
	id    int
	name  string
	email string
}

func fetchReplicaTestRows(t *testing.T, ctx context.Context, conn pglib.Querier, table string, withEmail bool) []replicaTestRow {
	t.Helper()

	query := fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", table)
	if withEmail {
		query = fmt.Sprintf("SELECT id, name, COALESCE(email,'') FROM %s ORDER BY id", table)
	}

	rows, err := conn.Query(ctx, query)
	if err != nil {
		// the table may not exist yet while the snapshot is still running
		return nil
	}
	defer rows.Close()

	result := []replicaTestRow{}
	for rows.Next() {
		row := replicaTestRow{}
		if withEmail {
			require.NoError(t, rows.Scan(&row.id, &row.name, &row.email))
		} else {
			require.NoError(t, rows.Scan(&row.id, &row.name))
		}
		result = append(result, row)
	}
	require.NoError(t, rows.Err())
	return result
}

func targetHasColumn(t *testing.T, ctx context.Context, conn pglib.Querier, table, column string) bool {
	t.Helper()

	var found bool
	err := conn.QueryRow(ctx, []any{&found}, `SELECT EXISTS(
		SELECT 1 FROM information_schema.columns
		WHERE table_name = $1 AND column_name = $2)`, table, column)
	require.NoError(t, err)
	return found
}
