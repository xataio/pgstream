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
	"github.com/xataio/pgstream/pkg/stream"
)

func Test_SnapshotToPostgres(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	// postgres container where pgstream hasn't been initialised to be used for
	// snapshot validation
	var snapshotPGURL string
	pgcleanup, err := testcontainers.SetupPostgresContainer(context.Background(), &snapshotPGURL, testcontainers.Postgres14, "config/postgresql.conf")
	require.NoError(t, err)
	defer pgcleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTable := "snapshot2pg_integration_test"
	// create table and populate it before initialising and running pgstream to
	// ensure the snapshot captures pre-existing schema and data properly
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("create table %s(id serial primary key, name text)", testTable))
	execQueryWithURL(t, ctx, snapshotPGURL, fmt.Sprintf("insert into %s(name) values('a'),('b')", testTable))

	cfg := &stream.Config{
		Listener:  testSnapshotListenerCfg(snapshotPGURL, targetPGURL, []string{testTable}),
		Processor: testPostgresProcessorCfg(snapshotPGURL),
	}
	initStream(t, ctx, snapshotPGURL)
	runStream(t, ctx, cfg)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)

	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	validation := func() bool {
		schemaColumns := getInformationSchemaColumns(t, ctx, targetConn, testTable)
		if len(schemaColumns) != 2 {
			return false
		}

		wantSchemaCols := []*informationSchemaColumn{
			{name: "id", dataType: "integer", isNullable: "NO"},
			{name: "name", dataType: "text", isNullable: "YES"},
		}
		require.ElementsMatch(t, wantSchemaCols, schemaColumns)

		columns := getTestTableColumns(t, ctx, targetConn, fmt.Sprintf("select id,name from %s", testTable))
		if len(columns) != 2 {
			return false
		}

		wantCols := []*testTableColumn{
			{id: 1, name: "a"},
			{id: 2, name: "b"},
		}
		require.ElementsMatch(t, wantCols, columns)

		return true
	}

	for {
		select {
		case <-timer.C:
			cancel()
			t.Error("timeout waiting for postgres snapshot sync")
			return
		case <-ticker.C:
			if validation() {
				return
			}
		}
	}
}
