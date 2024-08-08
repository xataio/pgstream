// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/kafka"
)

type Reader struct {
	FetchMessageFn  func(ctx context.Context) (*kafka.Message, error)
	CommitOffsetsFn func(ctx context.Context, offsets ...*kafka.Offset) error
	CloseFn         func() error
}

func (m *Reader) FetchMessage(ctx context.Context) (*kafka.Message, error) {
	return m.FetchMessageFn(ctx)
}

func (m *Reader) CommitOffsets(ctx context.Context, offsets ...*kafka.Offset) error {
	return m.CommitOffsetsFn(ctx, offsets...)
}

func (m *Reader) Close() error {
	return m.CloseFn()
}
