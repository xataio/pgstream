package mocks

import (
	"context"
	"sync/atomic"

	"github.com/xataio/pgstream/pkg/schemalog"
)

type Store struct {
	FetchFn    func(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error)
	AckFn      func(ctx context.Context, le *schemalog.LogEntry) error
	CloseFn    func() error
	fetchCalls uint64
	ackCalls   uint64
}

var _ schemalog.Store = (*Store)(nil)

func (m *Store) Fetch(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
	atomic.AddUint64(&m.fetchCalls, 1)
	return m.FetchFn(ctx, schemaName, ackedOnly)
}

func (m *Store) Ack(ctx context.Context, le *schemalog.LogEntry) error {
	atomic.AddUint64(&m.ackCalls, 1)
	return m.AckFn(ctx, le)
}

func (m *Store) Close() error {
	return m.CloseFn()
}

func (m *Store) GetFetchCalls() uint64 {
	return atomic.LoadUint64(&m.fetchCalls)
}

func (m *Store) GetAckCalls() uint64 {
	return atomic.LoadUint64(&m.ackCalls)
}
