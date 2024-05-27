// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"
	"sync/atomic"
)

type WeightedSemaphore struct {
	TryAcquireFn func(int64) bool
	AcquireFn    func(context.Context, int64) error
	ReleaseFn    func(uint64, int64)
	releaseCalls uint64
}

func (m *WeightedSemaphore) TryAcquire(i int64) bool {
	return m.TryAcquireFn(i)
}

func (m *WeightedSemaphore) Acquire(ctx context.Context, i int64) error {
	return m.AcquireFn(ctx, i)
}

func (m *WeightedSemaphore) Release(i int64) {
	atomic.AddUint64(&m.releaseCalls, 1)
	m.ReleaseFn(m.GetReleaseCalls(), i)
}

func (m *WeightedSemaphore) GetReleaseCalls() uint64 {
	return atomic.LoadUint64(&m.releaseCalls)
}
