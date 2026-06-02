// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
)

// BenchmarkCopyFrom compares pgx's binary-format COPY (Txn.CopyFrom) against
// the text-format implementation (Txn.CopyFromText) on the same row payload.
//
// Two table shapes:
//   - numeric: int8/timestamp/uuid columns — binary's strongest case (smaller
//     wire payload, no per-type input function on the destination).
//   - text:    text/varchar/jsonb columns — the gap shrinks because both
//     modes ultimately route through text-like decoding.
//
// Each benchmark runs one COPY of `rows` rows per iteration. The container
// is spun up once for the whole run and reused.
//
// Gated on PGSTREAM_INTEGRATION_TESTS so `go test ./...` doesn't pay the
// container cost.
//
// Usage:
//
//	PGSTREAM_INTEGRATION_TESTS=true go test -bench=BenchmarkCopyFrom \
//	    -benchtime=10x -run=^$ ./internal/postgres/
func BenchmarkCopyFrom(b *testing.B) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		b.Skip("set PGSTREAM_INTEGRATION_TESTS=true to run")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(b, err)
	defer func() { _ = cleanup() }()

	pool, err := pglib.NewConnPool(ctx, pgURL)
	require.NoError(b, err)
	defer pool.Close(ctx)

	// the text path encodes citext through cube_in / citext_in / ltree_in,
	// so install the extension on the bench db too — even though the
	// numeric/text benchmark shapes below don't use them, the codec map
	// is shared with the rest of the repo.
	for _, ext := range []string{"cube", "citext", "ltree"} {
		_, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS "+ext)
		require.NoError(b, err)
	}

	shapes := []struct {
		name    string
		create  string
		columns []string
		row     func(int) []any
	}{
		{
			name: "numeric_heavy",
			create: `CREATE TABLE bench_numeric (
				id          bigint PRIMARY KEY,
				amount      bigint NOT NULL,
				created_at  timestamptz NOT NULL,
				external_id uuid NOT NULL
			)`,
			columns: []string{"id", "amount", "created_at", "external_id"},
			row: func(i int) []any {
				return []any{
					int64(i),
					int64(i * 1000),
					time.Unix(int64(1_700_000_000+i), 0).UTC(),
					uuid.New().String(),
				}
			},
		},
		{
			name: "text_heavy",
			create: `CREATE TABLE bench_text (
				id     bigint PRIMARY KEY,
				name   text   NOT NULL,
				bio    text   NOT NULL,
				doc    jsonb  NOT NULL
			)`,
			columns: []string{"id", "name", "bio", "doc"},
			row: func(i int) []any {
				return []any{
					int64(i),
					fmt.Sprintf("user-%d", i),
					"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Curabitur eu congue arcu.",
					fmt.Sprintf(`{"i":%d,"tags":["a","b","c"],"nested":{"x":1.5,"y":"z"}}`, i),
				}
			},
		},
	}

	for _, shape := range shapes {
		shape := shape
		// reset table once per shape so benchmarks share state
		_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS bench_numeric, bench_text")
		require.NoError(b, err)
		_, err = pool.Exec(ctx, shape.create)
		require.NoError(b, err)

		for _, rows := range []int{1_000, 10_000} {
			payload := make([][]any, rows)
			for i := range payload {
				payload[i] = shape.row(i)
			}

			tableName := "bench_numeric"
			if shape.name == "text_heavy" {
				tableName = "bench_text"
			}

			b.Run(fmt.Sprintf("%s/binary/n=%d", shape.name, rows), func(b *testing.B) {
				runCopyBench(ctx, b, pool, tableName, shape.columns, payload, false)
			})
			b.Run(fmt.Sprintf("%s/text/n=%d", shape.name, rows), func(b *testing.B) {
				runCopyBench(ctx, b, pool, tableName, shape.columns, payload, true)
			})
		}
	}
}

func runCopyBench(ctx context.Context, b *testing.B, pool *pglib.Pool, table string, cols []string, rows [][]any, useText bool) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(approxRowBytes(rows))
	for b.Loop() {
		err := pool.ExecInTx(ctx, func(tx pglib.Tx) error {
			// TRUNCATE keeps every iteration writing into an empty table
			// without paying ALTER / VACUUM overhead between runs.
			if _, err := tx.Exec(ctx, "TRUNCATE "+table); err != nil {
				return err
			}
			if useText {
				_, err := tx.CopyFromText(ctx, table, cols, rows)
				return err
			}
			_, err := tx.CopyFrom(ctx, table, cols, rows)
			return err
		})
		require.NoError(b, err)
	}
}

// approxRowBytes returns a rough byte count for the payload so b.SetBytes
// produces a meaningful MB/s figure. We only need an order-of-magnitude
// approximation, hence the rough sizing of non-string values.
func approxRowBytes(rows [][]any) int64 {
	if len(rows) == 0 {
		return 0
	}
	const inlineNumericBytes = 8
	var perRow int64
	for _, v := range rows[0] {
		switch val := v.(type) {
		case string:
			perRow += int64(len(val))
		case []byte:
			perRow += int64(len(val))
		default:
			_ = val
			perRow += inlineNumericBytes
		}
	}
	return perRow * int64(len(rows))
}
