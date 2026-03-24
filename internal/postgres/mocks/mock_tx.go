// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/xataio/pgstream/internal/postgres"
)

type Tx struct {
	QueryRowFn    func(ctx context.Context, dest []any, query string, args ...any) error
	QueryFn       func(ctx context.Context, query string, args ...any) (postgres.Rows, error)
	ExecFn        func(ctx context.Context, i uint, query string, args ...any) (postgres.CommandTag, error)
	CopyFromFn    func(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error)
	SendBatchFn   func(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
	execCallCount uint
}

func (m *Tx) QueryRow(ctx context.Context, dest []any, query string, args ...any) error {
	return m.QueryRowFn(ctx, dest, query, args...)
}

func (m *Tx) Query(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
	return m.QueryFn(ctx, query, args...)
}

func (m *Tx) Exec(ctx context.Context, query string, args ...any) (postgres.CommandTag, error) {
	m.execCallCount++
	return m.ExecFn(ctx, m.execCallCount, query, args...)
}

func (m *Tx) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return m.SendBatchFn(ctx, batch)
}

func (m *Tx) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (rowCount int64, err error) {
	return m.CopyFromFn(ctx, tableName, columnNames, srcRows)
}
