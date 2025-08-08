// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"strings"
)

type SchemaTableMap map[string]map[string]struct{}

const (
	PublicSchema = "public"

	wildcard = "*"
)

var ErrInvalidTableName = errors.New("invalid table name format")

func NewSchemaTableMap(tables []string) (SchemaTableMap, error) {
	schemaTablesMap := make(SchemaTableMap, len(tables))
	for _, table := range tables {
		schemaName, tableName, err := parseTableName(table)
		if err != nil {
			return nil, err
		}
		if _, found := schemaTablesMap[schemaName]; !found {
			schemaTablesMap[schemaName] = make(map[string]struct{})
		}
		schemaTablesMap[schemaName][tableName] = struct{}{}
	}
	return schemaTablesMap, nil
}

func (t SchemaTableMap) ContainsSchemaTable(schema, table string) bool {
	if len(t) == 0 {
		return false
	}

	tables := t.GetSchemaTables(schema)
	if len(tables) == 0 {
		return false
	}

	_, found := tables[table]
	_, wildcardFound := tables[wildcard]
	return found || wildcardFound
}

func (t SchemaTableMap) GetSchemaTables(schema string) map[string]struct{} {
	tables, found := t[schema]
	if !found {
		return t[wildcard]
	}

	// make sure it's merged with the wildcard schema tables if any
	for table := range t[wildcard] {
		tables[table] = struct{}{}
	}

	return tables
}

func (t SchemaTableMap) Add(table string) error {
	schema, table, err := parseTableName(table)
	if err != nil {
		return err
	}
	_, found := t[schema]
	if !found {
		t[schema] = map[string]struct{}{}
	}
	t[schema][table] = struct{}{}
	return nil
}

func parseTableName(qualifiedTableName string) (string, string, error) {
	parts := strings.Split(qualifiedTableName, ".")
	switch len(parts) {
	case 1:
		return PublicSchema, parts[0], nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", ErrInvalidTableName
	}
}
