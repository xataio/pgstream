// SPDX-License-Identifier: Apache-2.0

package snapshot

import "errors"

type Errors struct {
	Snapshot error        `json:"snapshot,omitempty"`
	Tables   []TableError `json:"tables,omitempty"`
}

type TableError struct {
	Table    string `json:"table"`
	ErrorMsg string `json:"error"`
}

func (e *Errors) Error() string {
	var err error
	if e.Snapshot != nil {
		err = e.Snapshot
	}

	for _, table := range e.Tables {
		if err == nil {
			err = table
			continue
		}
		err = errors.Join(err, table)
	}
	return err.Error()
}

func (e *Errors) IsTableError(table string) bool {
	if e == nil {
		return false
	}

	if e.Snapshot != nil {
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
	return "snapshot error for table " + e.Table + ": " + e.ErrorMsg
}
