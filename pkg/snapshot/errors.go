// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"errors"
	"strings"
)

type Errors struct {
	SnapshotErrMsgs []string     `json:"snapshot,omitempty"`
	Tables          []TableError `json:"tables,omitempty"`
}

type TableError struct {
	Table    string `json:"table"`
	ErrorMsg string `json:"error"`
}

func NewErrors(err error) *Errors {
	if err == nil {
		return nil
	}
	var snapshotErrs *Errors
	if errors.As(err, &snapshotErrs) {
		return snapshotErrs
	}
	return &Errors{
		SnapshotErrMsgs: []string{err.Error()},
	}
}

func (e *Errors) Error() string {
	if e == nil {
		return ""
	}
	errMsgs := make([]string, 0, len(e.SnapshotErrMsgs))
	errMsgs = append(errMsgs, e.SnapshotErrMsgs...)

	for _, table := range e.Tables {
		errMsgs = append(errMsgs, table.Error())
	}
	return strings.Join(errMsgs, ";")
}

func (e *Errors) AddSnapshotError(err error) {
	if e == nil {
		return
	}
	if e.SnapshotErrMsgs == nil {
		e.SnapshotErrMsgs = []string{err.Error()}
		return
	}
	e.SnapshotErrMsgs = append(e.SnapshotErrMsgs, err.Error())
}

func (e *Errors) IsSnapshotError() bool {
	return e != nil && len(e.SnapshotErrMsgs) > 0
}

func (e *Errors) IsTableError(table string) bool {
	if e == nil {
		return false
	}

	if e.SnapshotErrMsgs != nil {
		return true
	}

	// treat wildcard table errors as true if there are any table errors.
	if table == "*" && len(e.Tables) > 0 {
		return true
	}

	for _, tableErr := range e.Tables {
		if tableErr.Table == table {
			return true
		}
	}
	return false
}

func (e *Errors) GetFailedTables() []string {
	if e == nil {
		return nil
	}
	failedTables := make([]string, 0, len(e.Tables))
	for _, table := range e.Tables {
		failedTables = append(failedTables, table.Table)
	}
	return failedTables
}

func NewTableError(table string, err error) TableError {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	return TableError{Table: table, ErrorMsg: errMsg}
}

func (e TableError) Error() string {
	return e.Table + ": " + e.ErrorMsg
}
