// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"context"
	"fmt"
	"sync"
)

// StoreCache is a wrapper around a schemalog Store that provides an in memory
// caching mechanism to reduce the amount of calls to the database. It is not
// concurrency safe.
type StoreCache struct {
	store Store
	mutex *sync.RWMutex
	cache map[string]*LogEntry
}

func NewStoreCache(store Store) *StoreCache {
	return &StoreCache{
		store: store,
		cache: make(map[string]*LogEntry),
		mutex: &sync.RWMutex{},
	}
}

func (s *StoreCache) Insert(ctx context.Context, schemaName string) (*LogEntry, error) {
	return s.store.Insert(ctx, schemaName)
}

func (s *StoreCache) FetchLast(ctx context.Context, schemaName string, ackedOnly bool) (*LogEntry, error) {
	logEntry := s.getCachedLogEntry(schemaName)
	if logEntry == nil {
		var err error
		logEntry, err = s.store.FetchLast(ctx, schemaName, ackedOnly)
		if err != nil {
			return nil, fmt.Errorf("store cache fetch last schema log: %w", err)
		}
		s.updateCachedLogEntry(schemaName, logEntry)
	}

	return logEntry, nil
}

func (s *StoreCache) Fetch(ctx context.Context, schemaName string, version int) (*LogEntry, error) {
	return s.store.Fetch(ctx, schemaName, version)
}

func (s *StoreCache) Ack(ctx context.Context, entry *LogEntry) error {
	s.updateCachedLogEntry(entry.SchemaName, entry)
	if err := s.store.Ack(ctx, entry); err != nil {
		return fmt.Errorf("store cache ack: %w", err)
	}
	return nil
}

func (s *StoreCache) Close() error {
	return s.store.Close()
}

func (s *StoreCache) getCachedLogEntry(schema string) *LogEntry {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.cache[schema]
}

func (s *StoreCache) updateCachedLogEntry(schema string, logEntry *LogEntry) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.cache[schema] = logEntry
}
