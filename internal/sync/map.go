// SPDX-License-Identifier: Apache-2.0

package sync

import (
	"maps"
	"sync"
)

type StringMap[T any] struct {
	m     map[string]T
	mutex *sync.RWMutex
}

func NewStringMap[T any]() *StringMap[T] {
	return &StringMap[T]{
		m:     make(map[string]T),
		mutex: &sync.RWMutex{},
	}
}

func NewStringMapWithLen[T any](len int) *StringMap[T] {
	return &StringMap[T]{
		m:     make(map[string]T, len),
		mutex: &sync.RWMutex{},
	}
}

func NewStringMapFromMap[T any](m map[string]T) *StringMap[T] {
	return &StringMap[T]{
		m:     m,
		mutex: &sync.RWMutex{},
	}
}

func (m *StringMap[T]) Get(key string) (T, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	value, ok := m.m[key]
	return value, ok
}

func (m *StringMap[T]) Set(key string, value T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[key] = value
}

func (m *StringMap[T]) Delete(key string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, key)
}

func (m *StringMap[T]) GetMap() map[string]T {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	result := make(map[string]T, len(m.m))
	maps.Copy(result, m.m)
	return result
}
