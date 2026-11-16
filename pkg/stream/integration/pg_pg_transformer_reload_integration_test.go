// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmdconfig "github.com/xataio/pgstream/cmd/config"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

// Test_PostgresToPostgres_TransformerReload exercises the same request channel
// that a SIGHUP feeds. The CLI signal handler is covered separately; this test
// proves that a published rules change reaches a running replication pipeline
// without restarting it.
func Test_PostgresToPostgres_TransformerReload(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	const testTable = "pg2pg_integration_transformer_reload_test"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createTable := fmt.Sprintf(`CREATE TABLE %s(id serial primary key, email text)`, testTable)
	execQuery(t, ctx, createTable)
	execQueryWithURL(t, ctx, targetPGURL, createTable)

	rulesFile := filepath.Join(t.TempDir(), "transformers.yaml")
	writeRules := func(literal string) {
		t.Helper()
		rules := fmt.Sprintf(`
transformations:
  validation_mode: strict
  table_transformers:
    - schema: public
      table: %s
      column_transformers:
        id: {}
        email:
          name: literal_string
          parameters:
            literal: %s
`, testTable, literal)
		require.NoError(t, os.WriteFile(rulesFile, []byte(rules), 0o600))
	}
	writeRules("before")

	load := cmdconfig.NewTransformerConfigLoader("", rulesFile)
	initialConfig, err := load(ctx)
	require.NoError(t, err)

	processorConfig := testPostgresProcessorCfg()
	processorConfig.Transformer = initialConfig
	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: processorConfig,
	}
	requests := make(chan struct{}, 1)
	runStream(t, ctx, cfg,
		stream.WithTransformerConfigLoader(load),
		stream.WithTransformerReloadRequests(requests),
	)

	sourceConn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer sourceConn.Close(ctx)

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	insert := func(email string) int {
		t.Helper()
		var id int
		require.NoError(t, sourceConn.QueryRow(ctx, []any{&id},
			fmt.Sprintf("INSERT INTO %s (email) VALUES ($1) RETURNING id", testTable), email))
		return id
	}
	targetEmail := func(id int) (string, error) {
		var email string
		err := targetConn.QueryRow(ctx, []any{&email},
			fmt.Sprintf("SELECT email FROM %s WHERE id = $1", testTable), id)
		return email, err
	}

	firstID := insert("first")
	require.Eventually(t, func() bool {
		email, err := targetEmail(firstID)
		return err == nil && email == "before"
	}, 30*time.Second, 100*time.Millisecond, "initial rules were not applied")

	// Publish the replacement rules and request a reload through the same path
	// a SIGHUP uses.
	writeRules("after")
	requests <- struct{}{}

	// Insert rows until one is processed after the reload. Each row is polled
	// for a short time before the next attempt, because the reload request and
	// the next WAL event are not ordered relative to each other.
	require.Eventually(t, func() bool {
		id := insert("after-reload")
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			email, err := targetEmail(id)
			if err == nil {
				return email == "after"
			}
			time.Sleep(25 * time.Millisecond)
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "reloaded rules were not applied")
}
