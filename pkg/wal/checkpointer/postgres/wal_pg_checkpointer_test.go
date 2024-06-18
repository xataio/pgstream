// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationmocks "github.com/xataio/pgstream/pkg/wal/replication/mocks"
)

func TestCheckpointer_SyncLSN(t *testing.T) {
	t.Parallel()

	mockParser := &replicationmocks.LSNParser{
		FromStringFn: func(s string) (replication.LSN, error) {
			switch s {
			case "1":
				return replication.LSN(1), nil
			case "2":
				return replication.LSN(2), nil
			case "3":
				return replication.LSN(3), nil
			default:
				return 0, fmt.Errorf("unsupported lsn format: %v", s)
			}
		},
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name   string
		pos    []wal.CommitPosition
		syncer lsnSyncer
		parser replication.LSNParser

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
			pos: []wal.CommitPosition{"1", "3", "2"},

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
			pos: []wal.CommitPosition{"1", "2", "3"},

			wantErr: errTest,
		},
		{
			name: "error - parsing lsn",
			syncer: &mockSyncer{
				syncLSNFn: func(ctx context.Context, lsn replication.LSN) error {
					return errors.New("syncLSNFn: should not be called")
				},
			},
			pos: []wal.CommitPosition{"1", "2", "3"},
			parser: &replicationmocks.LSNParser{
				FromStringFn: func(s string) (replication.LSN, error) {
					return 0, errTest
				},
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
				parser: mockParser,
			}

			if tc.parser != nil {
				c.parser = tc.parser
			}

			err := c.SyncLSN(context.Background(), tc.pos)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
