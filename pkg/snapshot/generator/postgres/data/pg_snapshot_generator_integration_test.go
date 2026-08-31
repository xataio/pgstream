// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/wal"
)

func Test_PostgresSnapshotGenerator(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14)
	require.NoError(t, err)
	defer pgCleanup()

	// use a mock row processor to validate the generator produces the right
	// rows
	mockProcessor := &mockProcessor{
		eventChan: make(chan *wal.Event),
	}

	generator, err := NewSnapshotGenerator(ctx, &Config{URL: pgurl}, mockProcessor)
	require.NoError(t, err)
	defer generator.Close()

	// create a table and populate it with data
	testTable := "snapshot_generator_integration_test"
	execQuery(t, ctx, pgurl, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id SERIAL PRIMARY KEY, name TEXT)", testTable))
	execQuery(t, ctx, pgurl, fmt.Sprintf("INSERT INTO %s(name) VALUES('alice')", testTable))
	execQuery(t, ctx, pgurl, fmt.Sprintf("INSERT INTO %s(name) VALUES('bob')", testTable))
	execQuery(t, ctx, pgurl, fmt.Sprintf("INSERT INTO %s(name) VALUES('charlie')", testTable))

	go func() {
		err = generator.CreateSnapshot(ctx, &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				"public": {testTable},
			},
		})
		require.NoError(t, err)
		mockProcessor.Close()
	}()

	rows := make([]*wal.Event, 0, 3)
	for event := range mockProcessor.eventChan {
		// the snapshot stamps wall-clock time
		event.Data.Timestamp = ""
		rows = append(rows, event)
	}

	wantEvents := []*wal.Event{
		newTestEvent(testTable, 1, "alice"),
		newTestEvent(testTable, 2, "bob"),
		newTestEvent(testTable, 3, "charlie"),
	}
	require.Equal(t, wantEvents, rows)
}

// added column must not read
func Test_PostgresSnapshotGenerator_pinnedColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgurl, testcontainers.Postgres14)
	require.NoError(t, err)
	defer pgCleanup()

	mockProcessor := &mockProcessor{
		eventChan: make(chan *wal.Event),
	}

	generator, err := NewSnapshotGenerator(ctx, &Config{URL: pgurl}, mockProcessor)
	require.NoError(t, err)
	defer generator.Close()

	testTable := "snapshot_generator_pinned_columns_test"
	execQuery(t, ctx, pgurl, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id SERIAL PRIMARY KEY, name TEXT)", testTable))
	execQuery(t, ctx, pgurl, fmt.Sprintf("INSERT INTO %s(name) VALUES('alice')", testTable))
	execQuery(t, ctx, pgurl, fmt.Sprintf("INSERT INTO %s(name) VALUES('bob')", testTable))

	execQuery(t, ctx, pgurl, fmt.Sprintf("ALTER TABLE %s ADD COLUMN added_later TEXT DEFAULT 'new'", testTable))

	// require here would hang
	var snapshotErr error
	go func() {
		defer mockProcessor.Close()
		snapshotErr = generator.CreateSnapshot(ctx, &snapshot.Snapshot{
			SchemaTables: map[string][]string{
				"public": {testTable},
			},
			TableColumns: pglib.SchemaTableColumns{
				"public": {testTable: {"id", "name"}},
			},
		})
	}()

	rows := make([]*wal.Event, 0, 2)
	for event := range mockProcessor.eventChan {
		rows = append(rows, event)
	}
	require.NoError(t, snapshotErr)

	require.Len(t, rows, 2)
	wantColumns := [][]wal.Column{
		{{Name: "id", Type: "int4", Value: int32(1)}, {Name: "name", Type: "text", Value: "alice"}},
		{{Name: "id", Type: "int4", Value: int32(2)}, {Name: "name", Type: "text", Value: "bob"}},
	}
	for i, row := range rows {
		require.Equal(t, wantColumns[i], row.Data.Columns)
	}
}

func newTestEvent(tableName string, id int32, name string) *wal.Event {
	return &wal.Event{
		CommitPosition: wal.CommitPosition(wal.ZeroLSN),
		Data: &wal.Data{
			Action: "I",
			LSN:    wal.ZeroLSN,
			Schema: "public",
			Table:  tableName,
			Columns: []wal.Column{
				{Name: "id", Type: "int4", Value: id},
				{Name: "name", Type: "text", Value: name},
			},
		},
	}
}
