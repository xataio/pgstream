// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
)

type QualifiedName struct {
	schema string
	name   string
}

var errUnexpectedQualifiedName = errors.New("unexpected qualified name format")

func NewQualifiedName(s string) (*QualifiedName, error) {
	qualifiedName := strings.Split(s, ".")
	switch len(qualifiedName) {
	case 1:
		return &QualifiedName{
			name: s,
		}, nil
	case 2:
		return &QualifiedName{
			schema: qualifiedName[0],
			name:   qualifiedName[1],
		}, nil
	default:
		return nil, errUnexpectedQualifiedName
	}
}

func (qn *QualifiedName) String() string {
	if qn.schema == "" {
		return qn.name
	}
	return QuoteQualifiedIdentifier(qn.schema, qn.name)
}

func (qn *QualifiedName) Schema() string {
	return qn.schema
}

func (qn *QualifiedName) Name() string {
	return qn.name
}

func QuoteIdentifier(s string) string {
	if IsQuotedIdentifier(s) {
		return s
	}
	return pq.QuoteIdentifier(s)
}

func QuoteQualifiedIdentifier(schema, table string) string {
	return QuoteIdentifier(schema) + "." + QuoteIdentifier(table)
}

func IsQuotedIdentifier(s string) bool {
	return len(s) > 2 && strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)
}

type (
	PGDumpFn    func(context.Context, PGDumpOptions) ([]byte, error)
	PGDumpAllFn func(context.Context, PGDumpAllOptions) ([]byte, error)
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
