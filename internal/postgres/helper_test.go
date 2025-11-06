// SPDX-License-Identifier: Apache-2.0

package postgres

import "context"

type mockQuerier struct {
	queryRowFn func(ctx context.Context, query string, args ...any) Row
}

func (m *mockQuerier) QueryRow(ctx context.Context, query string, args ...any) Row {
	return m.queryRowFn(ctx, query, args...)
}

func (m *mockQuerier) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	return nil, nil
}

func (m *mockQuerier) Exec(ctx context.Context, query string, args ...any) (CommandTag, error) {
	return CommandTag{}, nil
}

func (m *mockQuerier) ExecInTx(ctx context.Context, fn func(tx Tx) error) error {
	return nil
}

func (m *mockQuerier) ExecInTxWithOptions(ctx context.Context, fn func(tx Tx) error, opts TxOptions) error {
	return nil
}

func (m *mockQuerier) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (rowCount int64, err error) {
	return 0, nil
}

func (m *mockQuerier) Ping(ctx context.Context) error {
	return nil
}

func (m *mockQuerier) Close(ctx context.Context) error {
	return nil
}

type mockRow struct {
	scanFn func(args ...any) error
}

func (m *mockRow) Scan(args ...any) error {
	return m.scanFn(args...)
}
