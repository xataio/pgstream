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

func Test_WithRawJSONDecoding(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	// json/jsonb values covering every shape the codec must keep byte-faithful:
	// the JSON null value (distinct from SQL NULL), objects, JSON scalar
	// strings, verbatim json text, and arrays (whose jsonb elements would carry
	// a version-byte prefix if fetched in binary format).
	const query = `SELECT
		'null'::jsonb,
		'{"a": 1}'::jsonb,
		'"FIRST"'::jsonb,
		NULL::jsonb,
		' {"keep":  "spacing"} '::json,
		ARRAY['null'::jsonb, '{"b": 2}'::jsonb, NULL],
		ARRAY['null'::json],
		NULL::jsonb[]`

	fetchValues := func(t *testing.T, pool *Pool) []any {
		rows, err := pool.Query(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		values, err := rows.Values()
		require.NoError(t, err)
		require.NoError(t, rows.Err())
		return values
	}

	t.Run("with raw json decoding - values decode to raw text", func(t *testing.T) {
		pool, err := NewConnPool(ctx, pgURL, WithRawJSONDecoding())
		require.NoError(t, err)
		defer pool.Close(ctx)

		values := fetchValues(t, pool)
		require.Equal(t, []any{
			"null",
			`{"a": 1}`,
			`"FIRST"`,
			nil,
			` {"keep":  "spacing"} `,
			[]any{"null", `{"b": 2}`, nil},
			[]any{"null"},
			nil,
		}, values)
	})

	t.Run("without raw json decoding - JSON null is lost to Go nil", func(t *testing.T) {
		pool, err := NewConnPool(ctx, pgURL)
		require.NoError(t, err)
		defer pool.Close(ctx)

		values := fetchValues(t, pool)
		// documents the default pgx behaviour the option exists to avoid: the
		// JSON null value decodes to Go nil, indistinguishable from SQL NULL.
		require.Nil(t, values[0])
		require.Nil(t, values[3])
		require.IsType(t, map[string]any{}, values[1])
	})
}
