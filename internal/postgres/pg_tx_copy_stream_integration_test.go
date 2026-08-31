// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/testcontainers"
	"golang.org/x/sync/errgroup"
)

func Test_CopyStream_roundTrip(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	conn, err := NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	const setup = `
CREATE TABLE src (
	id       int primary key,
	label    text,
	amount   numeric,
	ts       timestamptz,
	flags    bool[],
	payload  bytea
);
INSERT INTO src VALUES
	(1, 'plain',                    '10.25',  '2024-01-02 03:04:05+00', '{t,f}', '\x00ff'),
	(2, E'tab\there',               '-0.001', '1999-12-31 23:59:59+00', '{}',    '\x'),
	(3, E'newline\nand\rreturn',    '0',      'infinity',               NULL,    NULL),
	(4, E'back\\slash',             NULL,     '-infinity',             '{t}',   '\xdeadbeef'),
	(5, '',                         '1e10',   '2024-06-01 12:00:00+00', '{f,f}', '\x0a0d09');
`
	_, err = conn.Exec(ctx, setup)
	require.NoError(t, err)

	tests := []struct {
		name   string
		format string
	}{
		{name: "text", format: ""},
		{name: "binary", format: " WITH (FORMAT binary)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := "dst_" + tc.name
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (LIKE src INCLUDING ALL)", target))
			require.NoError(t, err)

			// each end holds a COPY open
			srcConn, err := NewConn(ctx, pgURL)
			require.NoError(t, err)
			defer srcConn.Close(ctx)
			dstConn, err := NewConn(ctx, pgURL)
			require.NoError(t, err)
			defer dstConn.Close(ctx)

			var rowsOut, rowsIn int64
			pr, pw := io.Pipe()
			eg, egCtx := errgroup.WithContext(ctx)

			eg.Go(func() error {
				err := srcConn.ExecInTx(egCtx, func(tx Tx) error {
					var err error
					rowsOut, err = tx.CopyToWriter(egCtx, pw,
						fmt.Sprintf("COPY (SELECT * FROM src ORDER BY id) TO STDOUT%s", tc.format))
					return err
				})
				pw.CloseWithError(err)
				return err
			})

			eg.Go(func() error {
				err := dstConn.ExecInTx(egCtx, func(tx Tx) error {
					var err error
					rowsIn, err = tx.CopyFromReader(egCtx, pr,
						fmt.Sprintf("COPY %s FROM STDIN%s", target, tc.format))
					return err
				})
				pr.CloseWithError(err)
				return err
			})

			require.NoError(t, eg.Wait())
			require.Equal(t, int64(5), rowsOut)
			require.Equal(t, rowsOut, rowsIn)

			var diffs int
			require.NoError(t, conn.QueryRow(ctx, []any{&diffs}, fmt.Sprintf(
				`SELECT count(*) FROM (
					(TABLE src EXCEPT ALL TABLE %s) UNION ALL (TABLE %s EXCEPT ALL TABLE src)
				) d`, target, target)))
			require.Zero(t, diffs, "rows differ after the copy round trip")
		})
	}
}

// a failing target must not deadlock
func Test_CopyFromReader_targetErrorUnblocksSource(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	conn, err := NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// too many rows to buffer
	_, err = conn.Exec(ctx, `CREATE TABLE wide AS SELECT g AS id, repeat('x', 512) AS pad FROM generate_series(1, 20000) g;
CREATE TABLE narrow (id int);`)
	require.NoError(t, err)

	srcConn, err := NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer srcConn.Close(ctx)
	dstConn, err := NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer dstConn.Close(ctx)

	pr, pw := io.Pipe()
	eg, egCtx := errgroup.WithContext(ctx)

	var srcErr error
	eg.Go(func() error {
		srcErr = srcConn.ExecInTx(egCtx, func(tx Tx) error {
			_, err := tx.CopyToWriter(egCtx, pw, "COPY (SELECT * FROM wide) TO STDOUT")
			return err
		})
		pw.CloseWithError(srcErr)
		// expected, so reported via srcErr
		return nil
	})

	// one column against two
	var dstErr error
	eg.Go(func() error {
		dstErr = dstConn.ExecInTx(egCtx, func(tx Tx) error {
			_, err := tx.CopyFromReader(egCtx, pr, "COPY narrow FROM STDIN")
			return err
		})
		pr.CloseWithError(dstErr)
		return nil
	})

	require.NoError(t, eg.Wait())
	require.Error(t, dstErr, "target COPY should reject the mismatched stream")
	require.Error(t, srcErr, "source COPY should be unblocked by the target failure")
	require.False(t, errors.Is(srcErr, context.DeadlineExceeded))
}
