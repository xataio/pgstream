// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"sync"

	loglib "github.com/xataio/pgstream/pkg/log"
)

// startTransformerReloadWorker consumes transformation reload requests for the
// lifetime of the stream. The returned stop function cancels the worker and
// waits for any in-flight reload to finish. A nil reloader or request channel
// disables the worker.
func startTransformerReloadWorker(ctx context.Context, reloader transformerReloader, requests <-chan struct{}, logger loglib.Logger) func() {
	if reloader == nil || requests == nil {
		return func() {}
	}
	if logger == nil {
		logger = loglib.NewNoopLogger()
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-requests:
				if !ok {
					return
				}
				if _, err := reloader.Reload(ctx); err != nil {
					logger.Error(err, "reloading transformation rules")
				}
			}
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			cancel()
			<-done
		})
	}
}
