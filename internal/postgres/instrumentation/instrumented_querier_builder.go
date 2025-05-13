// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/otel"
)

func NewQuerierBuilder(b pglib.QuerierBuilder, i *otel.Instrumentation) (pglib.QuerierBuilder, error) {
	return func(ctx context.Context, url string) (pglib.Querier, error) {
		querier, err := b(ctx, url)
		if err != nil {
			return nil, err
		}
		return NewQuerier(querier, i)
	}, nil
}
