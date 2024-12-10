// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
)

type mockRowProcessor struct {
	rowChan chan *snapshot.Row
	once    sync.Once
}

func (mp *mockRowProcessor) process(ctx context.Context, row *snapshot.Row) error {
	mp.rowChan <- row
	return nil
}

func (mp *mockRowProcessor) close() {
	mp.once.Do(func() { close(mp.rowChan) })
}

type cleanup func() error

func setupPostgresContainer(ctx context.Context) (cleanup, string, error) {
	waitForLogs := wait.
		ForLog("database system is ready to accept connections").
		WithOccurrence(2).
		WithStartupTimeout(5 * time.Second)

	ctr, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("debezium/postgres:14-alpine"),
		testcontainers.WithWaitStrategy(waitForLogs),
	)
	if err != nil {
		return nil, "", fmt.Errorf("failed to start postgres container: %w", err)
	}

	pgurl, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", fmt.Errorf("retrieving connection string for postgres container: %w", err)
	}

	return func() error {
		return ctr.Terminate(ctx)
	}, pgurl, nil
}

func execQuery(t *testing.T, ctx context.Context, pgurl, query string) {
	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, query)
	require.NoError(t, err)
}
