// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/mocks"
)

func TestSubscriptionStoreCache_NewSubscriptionStoreCache(t *testing.T) {
	t.Parallel()

	subscription1 := newTestSubscription("url-1", "", "", []string{"D"})
	subscription2 := newTestSubscription("url-2", "my_schema", "my_table", []string{"I"})

	tests := []struct {
		name  string
		store store.Store

		wantCache map[string]*subscription.Subscription
		wantErr   error
	}{
		{
			name: "ok",
			store: &mocks.Store{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
					return []*subscription.Subscription{subscription1, subscription2}, nil
				},
			},
			wantCache: map[string]*subscription.Subscription{
				subscription1.Key(): subscription1,
				subscription2.Key(): subscription2,
			},
			wantErr: nil,
		},
		{
			name: "error - refreshing cache",
			store: &mocks.Store{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
					return nil, errTest
				},
			},
			wantCache: map[string]*subscription.Subscription{},
			wantErr:   errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cacheStore, err := New(context.Background(), tc.store, &Config{})
			require.ErrorIs(t, err, tc.wantErr)
			if err == nil {
				require.Equal(t, tc.wantCache, cacheStore.cache)
			}
		})
	}
}

func TestSubscriptionStoreCache_GetSubscriptions(t *testing.T) {
	t.Parallel()

	testSubscription1 := newTestSubscription("test-url-1", "test_schema", "test_table", []string{"D"})
	testSubscription2 := newTestSubscription("test-url-2", "test_schema", "test_table", []string{"I"})
	testSubscription3 := newTestSubscription("test-url-3", "", "", []string{"I"})

	tests := []struct {
		name   string
		store  store.Store
		action string
		schema string
		table  string

		wantSubscriptions []*subscription.Subscription
		wantErr           error
	}{
		{
			name:   "ok",
			action: "I",

			wantSubscriptions: []*subscription.Subscription{
				testSubscription2,
				testSubscription3,
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cacheStore := &Store{
				inner:     tc.store,
				cacheLock: &sync.RWMutex{},
				cache: map[string]*subscription.Subscription{
					testSubscription1.Key(): testSubscription1,
					testSubscription2.Key(): testSubscription2,
					testSubscription3.Key(): testSubscription3,
				},
			}

			subscriptions, err := cacheStore.GetSubscriptions(context.Background(), tc.action, tc.schema, tc.table)
			require.ErrorIs(t, err, tc.wantErr)
			require.ElementsMatch(t, tc.wantSubscriptions, subscriptions)
		})
	}
}
