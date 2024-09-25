// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"context"
	"fmt"
)

// StoreCache is a wrapper around a schemalog Store that provides an in memory
// caching mechanism to reduce the amount of calls to the database. It is not
// concurrency safe.
type StoreCache struct {
	store Store
	cache map[string]*LogEntry
}

func NewStoreCache(store Store) *StoreCache {
	return &StoreCache{
		store: store,
		cache: make(map[string]*LogEntry),
	}
}

func (s *StoreCache) Insert(ctx context.Context, schemaName string) (*LogEntry, error) {
	return s.store.Insert(ctx, schemaName)
}

func (s *StoreCache) Fetch(ctx context.Context, schemaName string, ackedOnly bool) (*LogEntry, error) {
	logEntry := s.cache[schemaName]
	if logEntry == nil {
		var err error
		logEntry, err = s.store.Fetch(ctx, schemaName, ackedOnly)
		if err != nil {
			return nil, fmt.Errorf("store cache fetch: %w", err)
		}
		s.cache[schemaName] = logEntry
	}

	return logEntry, nil
}

func (s *StoreCache) Ack(ctx context.Context, entry *LogEntry) error {
	s.cache[entry.SchemaName] = entry
	if err := s.store.Ack(ctx, entry); err != nil {
		return fmt.Errorf("store cache ack: %w", err)
	}
	return nil
}

func (s *StoreCache) Close() error {
	return s.store.Close()
}
