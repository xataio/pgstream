// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
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

func newIdentifier(tableName string) (pgx.Identifier, error) {
	var identifier pgx.Identifier
	qualifiedTableName := strings.Split(tableName, ".")
	switch len(qualifiedTableName) {
	case 1:
		identifier = pgx.Identifier{tableName}
	case 2:
		identifier = pgx.Identifier{qualifiedTableName[0], qualifiedTableName[1]}
	default:
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	// Remove any quotes from the table name. Identifier has a `Sanitize` method
	// that will be called and will add quotes, so if there are existing ones,
	// it will produce an invalid identifier name.
	for i, part := range identifier {
		identifier[i] = removeQuotes(part)
	}

	return identifier, nil
}

func removeQuotes(s string) string {
	return strings.Trim(s, `"`)
}

func extractDatabase(url string) (string, error) {
	pgCfg, err := pgx.ParseConfig(url)
	if err != nil {
		return "", err
	}
	return pgCfg.Database, nil
}
