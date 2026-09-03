// SPDX-License-Identifier: Apache-2.0

package lookup

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibmocks "github.com/xataio/pgstream/internal/postgres/mocks"
)

// StubOptions configures the failure a StubQuerier injects. The zero value
// serves the values successfully.
type StubOptions struct {
	NewQuerierErr error
	QueryErr      error
	ScanErr       error
	RowsErr       error
	// Closed reports whether the querier and its rows were closed
	Closed *bool
	// Query captures the SQL the loader built
	Query *string
}

// StubQuerier returns a NewQuerier replacement serving the given values as a
// single column of the given pg type OID. It lives beside the seam so that
// every package testing a lookup based transformer shares one test double.
func StubQuerier(oid uint32, values []any, opts StubOptions) func(context.Context, string) (pglib.Querier, error) {
	return func(context.Context, string) (pglib.Querier, error) {
		if opts.NewQuerierErr != nil {
			return nil, opts.NewQuerierErr
		}
		return &pglibmocks.Querier{
			QueryFn: func(_ context.Context, _ uint, query string, _ ...any) (pglib.Rows, error) {
				if opts.Query != nil {
					*opts.Query = query
				}
				if opts.QueryErr != nil {
					return nil, opts.QueryErr
				}
				return &pglibmocks.Rows{
					FieldDescriptionsFn: func() []pgconn.FieldDescription {
						return []pgconn.FieldDescription{{Name: "lookup", DataTypeOID: oid}}
					},
					NextFn: func(i uint) bool { return i <= uint(len(values)) },
					ScanFn: func(i uint, dest ...any) error {
						if opts.ScanErr != nil {
							return opts.ScanErr
						}
						value, ok := dest[0].(*any)
						if !ok {
							return errors.New("unexpected scan destination")
						}
						*value = values[i-1]
						return nil
					},
					ErrFn:   func() error { return opts.RowsErr },
					CloseFn: func() {},
				}, nil
			},
			CloseFn: func(context.Context) error {
				if opts.Closed != nil {
					*opts.Closed = true
				}
				return nil
			},
		}, nil
	}
}
