// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
)

type Snapshot struct {
	SchemaName string
	TableNames []string
}

type Request struct {
	Snapshot Snapshot
	Status   Status
	Errors   *Errors
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

type Status string

const (
	StatusRequested  = Status("requested")
	StatusInProgress = Status("in progress")
	StatusCompleted  = Status("completed")
)

func (s *Snapshot) IsValid() bool {
	return s != nil && s.SchemaName != "" && len(s.TableNames) > 0
}

func (r *Request) MarkCompleted(err error) {
	r.Status = StatusCompleted
	r.Errors = NewErrors(err)
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
