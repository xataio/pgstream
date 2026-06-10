// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/pgvector/pgvector-go"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/testcontainers"
)

func Test_registerTypesToConnMap_pgvector(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	t.Run("extension installed - vector round-trips through pgx", func(t *testing.T) {
		var pgURL string
		cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.PgvectorPostgres17)
		require.NoError(t, err)
		defer cleanup()

		bootstrap, err := NewConn(ctx, pgURL)
		require.NoError(t, err)
		_, err = bootstrap.Exec(ctx, "CREATE EXTENSION vector")
		require.NoError(t, err)
		bootstrap.Close(ctx)

		// New connection after the extension is installed: registerTypesToConnMap
		// should see vectorOID != 0 and call pgxvec.RegisterTypes successfully.
		conn, err := NewConn(ctx, pgURL)
		require.NoError(t, err)
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, "CREATE TABLE items (id serial primary key, embedding vector(3))")
		require.NoError(t, err)

		want := pgvector.NewVector([]float32{1, 2, 3})
		_, err = conn.Exec(ctx, "INSERT INTO items (embedding) VALUES ($1)", want)
		require.NoError(t, err)

		var got pgvector.Vector
		err = conn.QueryRow(ctx, []any{&got}, "SELECT embedding FROM items LIMIT 1")
		require.NoError(t, err)
		require.Equal(t, want.Slice(), got.Slice())
	})

	t.Run("extension not installed - connection still succeeds", func(t *testing.T) {
		var pgURL string
		cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
		require.NoError(t, err)
		defer cleanup()

		// registerTypesToConnMap should see vectorOID == 0 and skip RegisterTypes
		// entirely. Connection setup must not fail.
		conn, err := NewConn(ctx, pgURL)
		require.NoError(t, err)
		defer conn.Close(ctx)

		var one int
		err = conn.QueryRow(ctx, []any{&one}, "SELECT 1")
		require.NoError(t, err)
		require.Equal(t, 1, one)
	})
}
