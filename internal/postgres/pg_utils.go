// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/lib/pq"
)

func QuoteIdentifier(s string) string {
	return pq.QuoteIdentifier(s)
}

func QuoteQualifiedIdentifier(schema, table string) string {
	return pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(table)
}

type (
	PGDumpFn    func(context.Context, PGDumpOptions) ([]byte, error)
	PGRestoreFn func(context.Context, PGRestoreOptions, []byte) (string, error)
)
