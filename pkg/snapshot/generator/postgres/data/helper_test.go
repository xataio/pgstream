// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

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

func execQuery(t *testing.T, ctx context.Context, pgurl, query string) {
	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, query)
	require.NoError(t, err)
}
