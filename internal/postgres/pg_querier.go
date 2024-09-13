// SPDX-License-Identifier: Apache-2.0

package postgres

import "context"

type Querier interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
	Exec(ctx context.Context, query string, args ...any) (CommandTag, error)
	Close(ctx context.Context) error
}
type mappedRow struct {
	inner Row
}

func (mr *mappedRow) Scan(dest ...any) error {
	err := mr.inner.Scan(dest...)
	return mapError(err)
}
