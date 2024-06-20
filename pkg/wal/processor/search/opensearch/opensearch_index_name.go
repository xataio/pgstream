// SPDX-License-Identifier: Apache-2.0

package opensearch

import (
	"fmt"
)

// IndexName represents an opensearch index name constructed from a schema name.
type IndexName interface {
	Name() string
	Version() int
	NameWithVersion() string
	SchemaName() string
}

type indexName struct {
	schemaName string
	version    int
}

func newDefaultIndexName(schemaName string) IndexName {
	return &indexName{
		schemaName: schemaName,
		version:    1,
	}
}

func (i indexName) SchemaName() string {
	return i.schemaName
}

// NameWithVersion represents the name of the index with the version number. This should
// generally not be needed, in favour of `Name`.
func (i indexName) NameWithVersion() string {
	return fmt.Sprintf("%s-%d", i.schemaName, i.version)
}

// Name returns the name we should use for querying the index.
func (i *indexName) Name() string {
	return i.schemaName
}

func (i *indexName) Version() int {
	return i.version
}
