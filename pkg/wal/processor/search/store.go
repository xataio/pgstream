// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"

	"github.com/xataio/pgstream/pkg/schemalog"
)

type Store interface {
	// schema operations
	GetLatestSchema(ctx context.Context, schemaName string) (*schemalog.LogEntry, error)
	CreateSchema(ctx context.Context, schemaName string) error
	DeleteSchema(ctx context.Context, schemaName string) error
	UpdateSchemaMapping(ctx context.Context, schemaName string, m *schemalog.LogEntry, d *schemalog.SchemaDiff) error
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
	Version *int
	Delete  bool
}

type DocumentError struct {
	Document Document
	Status   int
	Error    string
}
