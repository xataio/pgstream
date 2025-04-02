// SPDX-License-Identifier: Apache-2.0

package postgres

import "github.com/lib/pq"

func QuoteIdentifier(s string) string {
	return pq.QuoteIdentifier(s)
}

func QuoteQualifiedIdentifier(schema, table string) string {
	return pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(table)
}
