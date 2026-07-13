// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"maps"
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

	containsTable := func(tables map[string]struct{}) bool {
		_, found := tables[table]
		_, wildcardFound := tables[wildcard]
		return found || wildcardFound
	}
	return containsTable(t[schema]) || containsTable(t[wildcard])
}

// ContainsExactSchemaTable returns true only if the table is listed by its
// exact name under the exact schema. Wildcard entries do not match.
func (t SchemaTableMap) ContainsExactSchemaTable(schema, table string) bool {
	_, found := t[schema][table]
	return found
}

func (t SchemaTableMap) GetSchemaTables(schema string) map[string]struct{} {
	tables, found := t[schema]
	if !found {
		return t[wildcard]
	}

	if len(t[wildcard]) == 0 {
		return tables
	}

	// merge with the wildcard schema tables into a copy, so the map itself is
	// never mutated by lookups
	merged := make(map[string]struct{}, len(tables)+len(t[wildcard]))
	maps.Copy(merged, tables)
	maps.Copy(merged, t[wildcard])
	return merged
}

// ValidateWildcardSchema returns an error when the wildcard schema entry lists
// anything other than the wildcard table ("*.*"): the snapshot generators
// can't resolve a specific table name across all schemas.
func (t SchemaTableMap) ValidateWildcardSchema() error {
	tables, found := t[wildcard]
	if !found {
		return nil
	}
	_, wildcardTableFound := tables[wildcard]
	if len(tables) != 1 || !wildcardTableFound {
		tableNames := make([]string, 0, len(tables))
		for table := range tables {
			tableNames = append(tableNames, table)
		}
		return fmt.Errorf("wildcard schema must be used with wildcard table, got %q", tableNames)
	}
	return nil
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
