// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

func TestMetricsCache_GetReplicationLag_Concurrency(t *testing.T) {
	t.Parallel()

	testCacheTTL := 100 * time.Millisecond
	var callCount atomic.Int64

	handler := &mocks.Handler{
		GetReplicationLagFn: func(ctx context.Context) (int64, error) {
			callCount.Add(1)
			// Simulate some processing time
			time.Sleep(10 * time.Millisecond)
			return 100, nil
		},
	}

	c := newMetricsCache(handler, testCacheTTL)

	// Pre-populate the cache to test concurrent reads from warm cache
	_, err := c.GetReplicationLag(context.Background())
	require.NoError(t, err)
	callCount.Store(0) // Reset counter after warming cache

	// Launch multiple concurrent goroutines
	numGoroutines := 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make([]int64, numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			lag, err := c.GetReplicationLag(context.Background())
			results[idx] = lag
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Verify all calls succeeded and returned the correct value
	for i := range numGoroutines {
		require.NoError(t, errors[i])
		require.Equal(t, int64(100), results[i])
	}

	// With warm cache, no additional calls should be made
	actualCalls := callCount.Load()
	require.Equal(t, int64(0), actualCalls, "expected 0 calls with warm cache, got %d", actualCalls)

	// Wait for cache to expire
	time.Sleep(testCacheTTL + 10*time.Millisecond)

	// Reset call counter
	callCount.Store(0)

	// Launch another batch to verify cache expiration works correctly
	// This tests the "thundering herd" scenario with cold cache
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			lag, err := c.GetReplicationLag(context.Background())
			results[idx] = lag
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Verify all calls succeeded
	for i := 0; i < numGoroutines; i++ {
		require.NoError(t, errors[i])
		require.Equal(t, int64(100), results[i])
	}

	// After cache expiration with cold cache, singleflight ensures only one
	// goroutine fetches fresh data while others wait for that result
	actualCalls = callCount.Load()
	require.Equal(t, int64(1), actualCalls, "expected exactly 1 call due to singleflight deduplication, got %d", actualCalls)
}
