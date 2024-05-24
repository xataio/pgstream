// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"

	"github.com/xataio/pgstream/pkg/schemalog"
)

type Store interface {
	GetMapper() Mapper
	// schema operations
	ApplySchemaChange(ctx context.Context, logEntry *schemalog.LogEntry) error
	DeleteSchema(ctx context.Context, schemaName string) error
	// data operations
	DeleteTableDocuments(ctx context.Context, schemaName string, tableIDs []string) error
	SendDocuments(ctx context.Context, docs []Document) ([]DocumentError, error)
}

type Mapper interface {
	ColumnToSearchMapping(column schemalog.Column) (map[string]any, error)
	MapColumnValue(column schemalog.Column, value any) (any, error)
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
