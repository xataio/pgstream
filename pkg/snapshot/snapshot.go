// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
)

type Snapshot struct {
	SchemaTables map[string][]string
}

type Request struct {
	Schema string
	Tables []string
	Status Status
	Errors *SchemaErrors
}

type Row struct {
	Schema  string
	Table   string
	Columns []Column
}

type Column struct {
	Name  string
	Type  string
	Value any
}

type RowProcessor func(context.Context, *Row) error

type RowsProcessor interface {
	ProcessRow(context.Context, *Row) error
	Close() error
}

type Status string

const (
	StatusRequested  = Status("requested")
	StatusInProgress = Status("in progress")
	StatusCompleted  = Status("completed")
)

func (s *Snapshot) GetSchemas() []string {
	if s == nil {
		return nil
	}

	schemas := make([]string, 0, len(s.SchemaTables))
	for schema := range s.SchemaTables {
		schemas = append(schemas, schema)
	}
	return schemas
}

func (s *Snapshot) GetTables() []string {
	if s == nil {
		return nil
	}

	tables := []string{}
	for schema, tables := range s.SchemaTables {
		for _, table := range tables {
			tables = append(tables, schema+"."+table)
		}
	}
	return tables
}

func (s *Snapshot) HasTables() bool {
	if s == nil {
		return false
	}

	for _, tables := range s.SchemaTables {
		if len(tables) > 0 {
			return true
		}
	}
	return false
}

func (r *Request) MarkCompleted(schema string, err error) {
	r.Status = StatusCompleted
	r.Errors = NewSchemaErrors(schema, err)
}

func (r *Request) MarkInProgress() {
	r.Status = StatusInProgress
}

func (r *Request) IsPending() bool {
	return r.Status == StatusRequested
}

func (r *Request) HasFailed() bool {
	return r.Status == StatusCompleted && r.Errors != nil
}

func (r *Request) HasFailedForTable(table string) bool {
	return r.Status == StatusCompleted && r.Errors.IsTableError(table)
}
