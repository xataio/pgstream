// SPDX-License-Identifier: Apache-2.0

package search

import (
	"errors"
	"fmt"
)

type ErrTypeInvalid struct {
	Input string
}

func (e ErrTypeInvalid) Error() string {
	return fmt.Sprintf("unsupported type: %s", e.Input)
}

type ErrSchemaNotFound struct {
	SchemaName string
}

func (e ErrSchemaNotFound) Error() string {
	return fmt.Sprintf("schema [%s] not found", e.SchemaName)
}

type ErrSchemaAlreadyExists struct {
	SchemaName string
}

func (e ErrSchemaAlreadyExists) Error() string {
	return fmt.Sprintf("schema [%s] already exists", e.SchemaName)
}

var (
	ErrRetriable = errors.New("retriable error")

	errVersionNotFound = errors.New("version column not found")
	errIDNotFound      = errors.New("id column not found")
	errNilIDValue      = errors.New("id has nil value")
	errNilVersionValue = errors.New("version has nil value")
)
