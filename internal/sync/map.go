// SPDX-License-Identifier: Apache-2.0

package sync

import (
	"maps"
	"sync"
)

type Map[T comparable, K any] struct {
	m     map[T]K
	mutex *sync.RWMutex
}

func NewMap[T comparable, K any]() *Map[T, K] {
	return &Map[T, K]{
		m:     make(map[T]K),
		mutex: &sync.RWMutex{},
	}
}

func NewMapWithLen[T comparable, K any](len int) *Map[T, K] {
	return &Map[T, K]{
		m:     make(map[T]K, len),
		mutex: &sync.RWMutex{},
	}
}

func NewMapFromMap[T comparable, K any](m map[T]K) *Map[T, K] {
	return &Map[T, K]{
		m:     m,
		mutex: &sync.RWMutex{},
	}
}

func (m *Map[T, K]) Get(key T) (K, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	value, ok := m.m[key]
	return value, ok
}

func (m *Map[T, K]) Set(key T, value K) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[key] = value
}

func (m *Map[T, K]) Delete(key T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, key)
}

func (m *Map[T, K]) GetMap() map[T]K {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	result := make(map[T]K, len(m.m))
	maps.Copy(result, m.m)
	return result
}
