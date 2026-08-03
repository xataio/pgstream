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

func Test_DiscoverTableColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	conn, err := NewConnPool(ctx, pgURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, ddl := range []string{
		`CREATE SCHEMA other_schema`,
		`CREATE TABLE public.discover_columns(zeta text, alpha int, to_drop text)`,
		`ALTER TABLE public.discover_columns DROP COLUMN to_drop`,
		`CREATE TABLE public."MixedCase"(id int)`,
		`CREATE TABLE other_schema.elsewhere(id int)`,
		`CREATE VIEW public.discover_view AS SELECT 1 AS x`,
	} {
		_, err = conn.Exec(ctx, ddl)
		require.NoError(t, err, ddl)
	}

	t.Run("all schemas", func(t *testing.T) {
		columns, err := DiscoverTableColumns(ctx, conn, nil, nil)
		require.NoError(t, err)

		require.Equal(t, []string{"zeta", "alpha"}, columns["public"]["discover_columns"])
		require.Equal(t, []string{"id"}, columns["public"]["MixedCase"])
		require.Equal(t, []string{"id"}, columns["other_schema"]["elsewhere"])
		require.NotContains(t, columns["public"], "discover_view")
		require.NotContains(t, columns, "pg_catalog")
	})

	t.Run("scoped to a schema", func(t *testing.T) {
		columns, err := DiscoverTableColumns(ctx, conn, []string{"other_schema"}, nil)
		require.NoError(t, err)

		require.Equal(t, SchemaTableColumns{"other_schema": {"elsewhere": {"id"}}}, columns)
	})

	t.Run("scoped to a schema and table", func(t *testing.T) {
		columns, err := DiscoverTableColumns(ctx, conn, []string{"public"}, []string{"discover_columns"})
		require.NoError(t, err)

		require.Equal(t, SchemaTableColumns{"public": {"discover_columns": {"zeta", "alpha"}}}, columns)
	})

	t.Run("quoted names in the scope resolve to the catalog entry", func(t *testing.T) {
		columns, err := DiscoverTableColumns(ctx, conn, []string{`"public"`}, []string{`"MixedCase"`})
		require.NoError(t, err)

		require.Equal(t, SchemaTableColumns{"public": {"MixedCase": {"id"}}}, columns)
		require.Equal(t, []string{"id"}, columns.ColumnsFor(`"public"`, `"MixedCase"`))
		require.Equal(t, []string{"id"}, columns.ColumnsFor("public", "MixedCase"))
	})
}
