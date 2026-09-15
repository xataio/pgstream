// SPDX-License-Identifier: Apache-2.0

// Package lookup holds the connection seam used by transformers that read
// their values from the database when they are built. It exists as its own
// package because pkg/transformers/builder's table driven tests construct
// every registered transformer and have no database, and they cannot reach an
// unexported variable in pkg/transformers.
package lookup

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
)

// NewQuerier opens the connection a lookup load runs on. It is a variable so
// that tests can build lookup based transformers without a database; replace
// it with StubQuerier.
var NewQuerier = func(ctx context.Context, url string) (pglib.Querier, error) {
	return pglib.NewConnPool(ctx, url)
}
