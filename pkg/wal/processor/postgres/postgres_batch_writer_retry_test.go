// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/internal/postgres/retrier"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
)

func TestBatchWriter_CommentedConcurrentDDLWithRetries(t *testing.T) {
	t.Parallel()

	const sql = "-- Migration indexes\nCREATE INDEX CONCURRENTLY test_idx ON public.test (id)"
	transactionAttempts, executions := 0, 0
	conn := &pgmocks.Querier{
		ExecInTxFn: func(ctx context.Context, fn func(pglib.Tx) error) error {
			transactionAttempts++
			return fn(&pgmocks.Tx{
				ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
					if query == prependSearchPath {
						return pglib.CommandTag{}, nil
					}
					require.Equal(t, sql, query)
					return pglib.CommandTag{}, &pglib.ErrActiveSQLTransaction{
						Details: "CREATE INDEX CONCURRENTLY cannot run inside a transaction block",
					}
				},
			})
		},
		ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
			executions++
			require.Equal(t, sql, query)
			return pglib.CommandTag{}, nil
		},
	}
	ctx := context.Background()
	logger := loglib.NewNoopLogger()
	retrying, err := retrier.NewQuerier(ctx, backoff.Config{
		Constant: &backoff.ConstantConfig{Interval: time.Millisecond, MaxRetries: 1},
	}, func(context.Context) (pglib.Querier, error) { return conn, nil }, logger)
	require.NoError(t, err)
	writer := &BatchWriter{Writer: &Writer{pgConn: retrying, logger: logger}}

	require.NoError(t, writer.execDDLQuery(ctx, &query{schema: "public", sql: sql, isDDL: true}))
	require.Equal(t, 1, executions)
	require.LessOrEqual(t, transactionAttempts, 1, "transaction-unsafe DDL must not be retried inside a transaction")
}
