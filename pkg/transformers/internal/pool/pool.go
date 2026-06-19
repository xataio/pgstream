// SPDX-License-Identifier: Apache-2.0

// Package pool provides a concurrency-safe pool of instances for libraries
// whose types are not safe for concurrent use. pgstream shares a single
// transformer per column across all snapshot worker goroutines, so
// transformers backed by stateful libraries (shared rng, internal buffers)
// hand out one underlying instance per concurrent caller instead of
// serializing calls with a lock.
package pool

import "sync"

// Pool hands out dedicated instances of T so that concurrent callers never
// share one. Instances are created lazily with the provided constructor and
// reused across calls once released.
type Pool[T any] struct {
	pool  sync.Pool
	newFn func() (T, error)
}

// New returns a pool that creates instances with newFn. The constructor is
// invoked eagerly once so that invalid configuration fails at build time
// rather than on first use.
func New[T any](newFn func() (T, error)) (*Pool[T], error) {
	instance, err := newFn()
	if err != nil {
		return nil, err
	}
	p := &Pool[T]{newFn: newFn}
	p.pool.Put(instance)
	return p, nil
}

// Acquire returns an instance for exclusive use by the caller. It must be
// returned with Release once the caller is done with it, including any reads
// of buffers the instance owns.
func (p *Pool[T]) Acquire() (T, error) {
	if v, ok := p.pool.Get().(T); ok {
		return v, nil
	}
	return p.newFn()
}

// Release returns an instance to the pool for reuse.
func (p *Pool[T]) Release(instance T) {
	p.pool.Put(instance)
}
