// SPDX-License-Identifier: Apache-2.0

package mocks

import "github.com/xataio/pgstream/internal/backoff"

type Backoff struct {
	RetryNotifyFn func(backoff.Operation, backoff.Notify) error
}

func (m *Backoff) RetryNotify(op backoff.Operation, not backoff.Notify) error {
	return m.RetryNotifyFn(op, not)
}
