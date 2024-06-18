// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal/replication"
)

type mockSyncer struct {
	syncLSNFn func(context.Context, replication.LSN) error
}

func (m *mockSyncer) SyncLSN(ctx context.Context, lsn replication.LSN) error {
	return m.syncLSNFn(ctx, lsn)
}

func (m *mockSyncer) Close() error {
	return nil
}
