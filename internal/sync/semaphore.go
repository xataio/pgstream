// SPDX-License-Identifier: Apache-2.0

package sync

import (
	"context"

	"golang.org/x/sync/semaphore"
)

type WeightedSemaphore interface {
	TryAcquire(int64) bool
	Acquire(context.Context, int64) error
	Release(int64)
}

func NewWeightedSemaphore(size int64) *semaphore.Weighted {
	return semaphore.NewWeighted(size)
}
