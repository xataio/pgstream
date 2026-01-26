// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
)

type Store interface {
	GetMapper() Mapper
	// schema operations
	ApplySchemaDiff(ctx context.Context, diff *wal.SchemaDiff) error
	DeleteSchema(ctx context.Context, schemaName string) error
	// data operations
	DeleteTableDocuments(ctx context.Context, schemaName string, tableIDs []string) error
	SendDocuments(ctx context.Context, docs []Document) ([]DocumentError, error)
}

type Mapper interface {
	ColumnToSearchMapping(column *wal.DDLColumn) (map[string]any, error)
	MapColumnValue(column *wal.Column) (any, error)
}

type Document struct {
	ID      string
	Schema  string
	Data    map[string]any
	Version int
	Delete  bool
}

type DocumentError struct {
	Document Document
	Severity Severity
	Error    string
}

type Severity uint

const (
	SeverityNone Severity = iota
	SeverityDataLoss
	SeverityIgnored
	SeverityRetriable
)

func (s *Severity) String() string {
	if s == nil {
		return ""
	}
	switch *s {
	case SeverityNone:
		return "NONE"
	case SeverityDataLoss:
		return "DATALOSS"
	case SeverityIgnored:
		return "IGNORED"
	case SeverityRetriable:
		return "RETRIABLE"
	default:
		return ""
	}
}
