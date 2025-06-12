// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"errors"
	"strings"
)

type (
	Errors       map[string]*SchemaErrors
	SchemaErrors struct {
		Schema       string            `json:"schema,omitempty"`
		GlobalErrors []string          `json:"snapshot,omitempty"`
		TableErrors  map[string]string `json:"tables,omitempty"`
	}
)

func NewErrors(schema string, err error) Errors {
	if err == nil {
		return nil
	}
	errs := make(Errors)
	if errors.As(err, &errs) {
		return errs
	}
	return Errors{
		schema: NewSchemaErrors(schema, err),
	}
}

func (e Errors) GetSchemaErrors(schema string) *SchemaErrors {
	if e == nil {
		return nil
	}

	if errs, found := e[schema]; found {
		return errs
	}
	return nil
}

func (e Errors) AddError(schema string, err error) {
	if e == nil {
		e = make(Errors)
	}

	schemaErrs, found := e[schema]
	if found {
		schemaErrs.AddGlobalError(err)
		return
	}
	e[schema] = NewSchemaErrors(schema, err)
}

func (e Errors) Error() string {
	var errMsg strings.Builder
	for _, errs := range e {
		errMsg.WriteString(errs.Error())
	}
	return errMsg.String()
}

func NewSchemaErrors(schema string, err error) *SchemaErrors {
	if err == nil {
		return nil
	}
	var schemaErrs *SchemaErrors
	if errors.As(err, &schemaErrs) {
		return schemaErrs
	}
	return &SchemaErrors{
		Schema:       schema,
		GlobalErrors: []string{err.Error()},
	}
}

func (e *SchemaErrors) Error() string {
	if e == nil {
		return ""
	}
	errMsgs := make([]string, 0, len(e.GlobalErrors))
	errMsgs = append(errMsgs, e.GlobalErrors...)

	for table, errMsg := range e.TableErrors {
		errMsgs = append(errMsgs, table+": "+errMsg)
	}
	return e.Schema + ": " + strings.Join(errMsgs, ";")
}

func (e *SchemaErrors) AddGlobalError(err error) {
	if e == nil {
		return
	}

	e.GlobalErrors = append(e.GlobalErrors, err.Error())
}

func (e *SchemaErrors) AddTableError(table string, err error) {
	if e == nil || err == nil {
		return
	}

	if e.TableErrors == nil {
		e.TableErrors = make(map[string]string)
	}

	e.TableErrors[table] = err.Error()
}

func (e *SchemaErrors) IsGlobalError() bool {
	return e != nil && len(e.GlobalErrors) > 0
}

func (e *SchemaErrors) IsTableError(table string) bool {
	if e == nil {
		return false
	}

	if len(e.GlobalErrors) > 0 {
		return true
	}

	// treat wildcard table errors as true if there are any table errors.
	if table == "*" && len(e.TableErrors) > 0 {
		return true
	}

	_, found := e.TableErrors[table]
	return found
}

func (e *SchemaErrors) GetFailedTables() []string {
	if e == nil {
		return nil
	}
	failedTables := make([]string, 0, len(e.TableErrors))
	for table := range e.TableErrors {
		failedTables = append(failedTables, table)
	}
	return failedTables
}
