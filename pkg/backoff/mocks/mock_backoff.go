// SPDX-License-Identifier: Apache-2.0

package mocks

import "github.com/xataio/pgstream/pkg/backoff"

type Backoff struct {
	RetryNotifyFn func(backoff.Operation, backoff.Notify) error
	RetryFn       func(backoff.Operation) error
}

func (m *Backoff) RetryNotify(op backoff.Operation, not backoff.Notify) error {
	return m.RetryNotifyFn(op, not)
}

func (m *Backoff) Retry(op backoff.Operation) error {
	return m.RetryFn(op)
}
