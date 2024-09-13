// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store"
)

// Store is a wrapper around a subscription store that keeps an in memory cache
// to minimise calls to the persistent store. It is concurrency safe. The cache
// contents will be refreshed on a configurable interval. This is an ephemeral
// lightweight wrapper that doesn't control memory usage. Should only be used
// when the amount of subscriptions is manageable for the resources allocated.
// The sync interval will represent in the worst case scenario the staleness of
// the cache.
type Store struct {
	inner        store.Store
	logger       loglib.Logger
	cacheLock    *sync.RWMutex
	cache        map[string]*subscription.Subscription
	syncInterval time.Duration
}

type Option func(*Store)

// NewStoreCache will wrap the store on input, providing a simple in memory
// cache to minimise calls to the persistent store. It will perform an initial
// warm up to retrieve all existing subscriptions, and will sync with the store
// on input on a configured interval.
func New(ctx context.Context, store store.Store, cfg *Config, opts ...Option) (*Store, error) {
	s := &Store{
		inner:        store,
		logger:       loglib.NewNoopLogger(),
		cache:        make(map[string]*subscription.Subscription),
		cacheLock:    &sync.RWMutex{},
		syncInterval: cfg.syncInterval(),
	}

	for _, opt := range opts {
		opt(s)
	}

	if err := s.refresh(ctx); err != nil {
		return nil, err
	}

	// start a go routine that will refresh the cache contents on the configured
	// interval.
	go s.syncRefresh(ctx)

	return s, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(sc *Store) {
		sc.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "subscription_store_cache",
		})
	}
}

func (s *Store) CreateSubscription(ctx context.Context, subscription *subscription.Subscription) error {
	return s.inner.CreateSubscription(ctx, subscription)
}

func (s *Store) DeleteSubscription(ctx context.Context, subscription *subscription.Subscription) error {
	return s.inner.DeleteSubscription(ctx, subscription)
}

func (s *Store) GetSubscriptions(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	subscriptions := make([]*subscription.Subscription, 0, len(s.cache))
	for _, subscription := range s.cache {
		if subscription.IsFor(action, schema, table) {
			subscriptions = append(subscriptions, subscription)
		}
	}

	return subscriptions, nil
}

func (s *Store) syncRefresh(ctx context.Context) {
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.refresh(ctx); err != nil {
				s.logger.Error(err, "refreshing store cache")
			}
		}
	}
}

func (s *Store) refresh(ctx context.Context) error {
	// get all subscriptions and populate the cache
	subscriptions, err := s.inner.GetSubscriptions(ctx, "", "", "")
	if err != nil {
		return fmt.Errorf("retrieving subscriptions: %w", err)
	}

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	s.cache = make(map[string]*subscription.Subscription, len(subscriptions))
	for _, subscription := range subscriptions {
		s.cache[subscription.Key()] = subscription
	}

	s.logger.Debug("cache refreshed", loglib.Fields{
		"subscription_total_count": len(s.cache),
		"subscriptions":            s.cache,
	})

	return nil
}
