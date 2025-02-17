// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/snapshot"
)

func Test_PostgresSnapshotGenerator(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgCleanup, pgurl, err := setupPostgresContainer(ctx)
	require.NoError(t, err)
	defer pgCleanup()

	// use a mock row processor to validate the generator produces the right
	// rows
	mockProcessor := &mockRowProcessor{
		rowChan: make(chan *snapshot.Row),
	}

	generator, err := NewSnapshotGenerator(ctx, &Config{URL: pgurl}, mockProcessor.process)
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
			SchemaName: "public",
			TableNames: []string{testTable},
		})
		require.NoError(t, err)
		mockProcessor.close()
	}()

	rows := make([]*snapshot.Row, 0, 3)
	for row := range mockProcessor.rowChan {
		rows = append(rows, row)
	}

	wantRows := []*snapshot.Row{
		newTestRow(testTable, 1, "alice"),
		newTestRow(testTable, 2, "bob"),
		newTestRow(testTable, 3, "charlie"),
	}
	require.Equal(t, wantRows, rows)
}

func newTestRow(tableName string, id int32, name string) *snapshot.Row {
	return &snapshot.Row{
		Schema: "public",
		Table:  tableName,
		Columns: []snapshot.Column{
			{Name: "id", Type: "int4", Value: id},
			{Name: "name", Type: "text", Value: name},
		},
	}
}
