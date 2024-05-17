// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/internal/kafka"
)

type Reader struct {
	FetchMessageFn   func(ctx context.Context) (*kafka.Message, error)
	CommitMessagesFn func(ctx context.Context, msgs ...*kafka.Message) error
	CloseFn          func() error
}

func (m *Reader) FetchMessage(ctx context.Context) (*kafka.Message, error) {
	return m.FetchMessageFn(ctx)
}

func (m *Reader) CommitMessages(ctx context.Context, msgs ...*kafka.Message) error {
	return m.CommitMessagesFn(ctx, msgs...)
}

func (m *Reader) Close() error {
	return m.CloseFn()
}
