// SPDX-License-Identifier: Apache-2.0

package store

import (
	"fmt"
	"strings"
)

type IndexNameAdapter interface {
	SchemaNameToIndex(schemaName string) IndexName
	IndexToSchemaName(index string) string
}

// IndexName represents an opensearch index name constructed from a schema name.
type IndexName interface {
	Name() string
	Version() int
	NameWithVersion() string
	SchemaName() string
}

type defaultIndexNameAdapter struct{}

func newDefaultIndexNameAdapter() IndexNameAdapter {
	return &defaultIndexNameAdapter{}
}

func (i *defaultIndexNameAdapter) SchemaNameToIndex(schemaName string) IndexName {
	return newDefaultIndexName(schemaName)
}

func (i *defaultIndexNameAdapter) IndexToSchemaName(index string) string {
	return strings.TrimSuffix(index, "-1")
}

type defaultIndexName struct {
	schemaName string
	version    int
}

func newDefaultIndexName(schemaName string) IndexName {
	return &defaultIndexName{
		schemaName: schemaName,
		version:    1,
	}
}

func (i defaultIndexName) SchemaName() string {
	return i.schemaName
}

// NameWithVersion represents the name of the index with the version number. This should
// generally not be needed, in favour of `Name`.
func (i defaultIndexName) NameWithVersion() string {
	return fmt.Sprintf("%s-%d", i.schemaName, i.version)
}

// Name returns the name we should use for querying the index.
func (i *defaultIndexName) Name() string {
	return i.schemaName
}

func (i *defaultIndexName) Version() int {
	return i.version
}
