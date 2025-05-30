// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/internal/postgres"
)

type Tx struct {
	QueryRowFn    func(ctx context.Context, query string, args ...any) postgres.Row
	QueryFn       func(ctx context.Context, query string, args ...any) (postgres.Rows, error)
	ExecFn        func(ctx context.Context, i uint, query string, args ...any) (postgres.CommandTag, error)
	CopyFromFn    func(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error)
	execCallCount uint
}

func (m *Tx) QueryRow(ctx context.Context, query string, args ...any) postgres.Row {
	return m.QueryRowFn(ctx, query, args...)
}

func (m *Tx) Query(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
	return m.QueryFn(ctx, query, args...)
}

func (m *Tx) Exec(ctx context.Context, query string, args ...any) (postgres.CommandTag, error) {
	m.execCallCount++
	return m.ExecFn(ctx, m.execCallCount, query, args...)
}

func (m *Tx) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (rowCount int64, err error) {
	return m.CopyFromFn(ctx, tableName, columnNames, srcRows)
}
