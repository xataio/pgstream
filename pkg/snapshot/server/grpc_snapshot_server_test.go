// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/api"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	snapshotstoremocks "github.com/xataio/pgstream/pkg/snapshot/store/mocks"
)

func TestGRPCServer_RequestSnapshot(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name  string
		store snapshotstore.Store

		wantErr error
	}{
		{
			name: "ok",
			store: &snapshotstoremocks.Store{
				CreateSnapshotRequestFn: func(ctx context.Context, s *snapshot.Snapshot) error {
					wantSnapshot := &snapshot.Snapshot{
						SchemaName:          "test-schema",
						TableName:           "test-table",
						IdentityColumnNames: []string{"id"},
					}
					require.Equal(t, wantSnapshot, s)
					return nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error - creating snapshot request",
			store: &snapshotstoremocks.Store{
				CreateSnapshotRequestFn: func(ctx context.Context, s *snapshot.Snapshot) error {
					return errTest
				},
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			grpcServer := GRPCServer{
				store:   tc.store,
				adapter: &adapter{},
				logger:  loglib.NewNoopLogger(),
			}
			_, err := grpcServer.RequestSnapshot(context.Background(), &api.SnapshotRequest{
				SchemaName:          "test-schema",
				TableName:           "test-table",
				IdentityColumnNames: []string{"id"},
			})
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
