// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/replication"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestCheckpointer_SyncLSN(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		pos    []wal.CommitPosition
		syncer lsnSyncer

		wantErr error
	}{
		{
			name: "ok",
			syncer: &mockSyncer{
				syncLSNFn: func(ctx context.Context, lsn replication.LSN) error {
					require.Equal(t, replication.LSN(3), lsn)
					return nil
				},
			},
			pos: []wal.CommitPosition{
				{PGPos: replication.LSN(1)},
				{PGPos: replication.LSN(3)},
				{PGPos: replication.LSN(2)},
			},

			wantErr: nil,
		},
		{
			name: "ok - empty positions",
			syncer: &mockSyncer{
				syncLSNFn: func(ctx context.Context, lsn replication.LSN) error {
					return errors.New("syncLSNFn: should not be called")
				},
			},
			pos: []wal.CommitPosition{},

			wantErr: nil,
		},
		{
			name: "error - syncing lsn",
			syncer: &mockSyncer{
				syncLSNFn: func(ctx context.Context, lsn replication.LSN) error {
					return errTest
				},
			},
			pos: []wal.CommitPosition{
				{PGPos: replication.LSN(1)},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := Checkpointer{
				syncer: tc.syncer,
			}
			err := c.SyncLSN(context.Background(), tc.pos)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
