// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	natslib "github.com/xataio/pgstream/pkg/nats"
)

type Reader struct {
	FetchMessageFn  func(ctx context.Context) (*natslib.Message, error)
	CommitOffsetsFn func(ctx context.Context, offsets ...*natslib.Offset) error
	CloseFn         func() error
}

func (m *Reader) FetchMessage(ctx context.Context) (*natslib.Message, error) {
	return m.FetchMessageFn(ctx)
}

func (m *Reader) CommitOffsets(ctx context.Context, offsets ...*natslib.Offset) error {
	return m.CommitOffsetsFn(ctx, offsets...)
}

func (m *Reader) Close() error {
	return m.CloseFn()
}
