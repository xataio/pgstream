// SPDX-License-Identifier: Apache-2.0

package postgres

import "context"

type QuerierBuilder func(context.Context, string) (Querier, error)

func ConnBuilder(ctx context.Context, url string) (Querier, error) {
	return NewConn(ctx, url)
}

func ConnPoolBuilder(ctx context.Context, url string) (Querier, error) {
	return NewConnPool(ctx, url)
}
