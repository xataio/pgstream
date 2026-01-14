// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal/replication"
	"github.com/xataio/pgstream/pkg/wal/replication/mocks"
)

func TestMetricsCache_GetReplicationLag(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")
	testCacheTTL := 1 * time.Minute

	tests := []struct {
		name           string
		cacheLag       int64
		cacheUpdatedAt time.Time
		handler        replication.Handler

		wantLag int64
		wantErr error
	}{
		{
			name: "returns fresh replication lag if cache is not set",
			handler: &mocks.Handler{
				GetReplicationLagFn: func(ctx context.Context) (int64, error) {
					return 100, nil
				},
			},
			wantLag: 100,
			wantErr: nil,
		},
		{
			name:           "returns cached replication lag if cache is valid",
			cacheLag:       30,
			cacheUpdatedAt: time.Now(),
			handler: &mocks.Handler{
				GetReplicationLagFn: func(ctx context.Context) (int64, error) {
					return 100, nil
				},
			},
			wantLag: 30,
			wantErr: nil,
		},
		{
			name:           "returns fresh replication lag if cache is invalid",
			cacheLag:       30,
			cacheUpdatedAt: time.Now().Add(-2 * time.Minute),
			handler: &mocks.Handler{
				GetReplicationLagFn: func(ctx context.Context) (int64, error) {
					return 100, nil
				},
			},
			wantLag: 100,
			wantErr: nil,
		},
		{
			name:           "error getting fresh replication lag",
			cacheLag:       30,
			cacheUpdatedAt: time.Now().Add(-2 * time.Minute),
			handler: &mocks.Handler{
				GetReplicationLagFn: func(ctx context.Context) (int64, error) {
					return -1, errTest
				},
			},
			wantLag: 0,
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMetricsCache(tc.handler, testCacheTTL)
			c.replicationLag = tc.cacheLag
			c.replicationLagUpdatedAt = tc.cacheUpdatedAt

			lag, err := c.GetReplicationLag(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantLag, lag)
		})
	}
}
