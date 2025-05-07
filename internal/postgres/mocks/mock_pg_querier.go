// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"
	"sync/atomic"

	"github.com/xataio/pgstream/internal/postgres"
)

type Querier struct {
	QueryRowFn               func(ctx context.Context, query string, args ...any) postgres.Row
	QueryFn                  func(ctx context.Context, query string, args ...any) (postgres.Rows, error)
	ExecFn                   func(context.Context, uint, string, ...any) (postgres.CommandTag, error)
	ExecInTxFn               func(context.Context, func(tx postgres.Tx) error) error
	ExecInTxWithOptionsFn    func(context.Context, uint, func(tx postgres.Tx) error, postgres.TxOptions) error
	PingFn                   func(context.Context) error
	CloseFn                  func(context.Context) error
	execCalls                uint32
	execInTxWithOptionsCalls uint32
}

func (m *Querier) QueryRow(ctx context.Context, query string, args ...any) postgres.Row {
	return m.QueryRowFn(ctx, query, args...)
}

func (m *Querier) Query(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
	return m.QueryFn(ctx, query, args...)
}

func (m *Querier) Exec(ctx context.Context, query string, args ...any) (postgres.CommandTag, error) {
	atomic.AddUint32(&m.execCalls, 1)
	return m.ExecFn(ctx, uint(atomic.LoadUint32(&m.execCalls)), query, args...)
}

func (m *Querier) ExecInTx(ctx context.Context, fn func(tx postgres.Tx) error) error {
	return m.ExecInTxFn(ctx, fn)
}

func (m *Querier) ExecInTxWithOptions(ctx context.Context, fn func(tx postgres.Tx) error, opts postgres.TxOptions) error {
	atomic.AddUint32(&m.execInTxWithOptionsCalls, 1)
	return m.ExecInTxWithOptionsFn(ctx, uint(atomic.LoadUint32(&m.execInTxWithOptionsCalls)), fn, opts)
}

func (m *Querier) Ping(ctx context.Context) error {
	if m.PingFn != nil {
		return m.PingFn(ctx)
	}
	return nil
}

func (m *Querier) Close(ctx context.Context) error {
	if m.CloseFn != nil {
		return m.CloseFn(ctx)
	}
	return nil
}
