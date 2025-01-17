// SPDX-License-Identifier: Apache-2.0

package search

import (
	"errors"
	"fmt"
	"time"
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

type ErrSchemaUpdateOutOfOrder struct {
	SchemaName       string
	SchemaID         string
	NewVersion       int
	CurrentVersion   int
	CurrentCreatedAt time.Time
	NewCreatedAt     time.Time
}

func (e ErrSchemaUpdateOutOfOrder) Error() string {
	return fmt.Sprintf("our of order schema update detected for schema [%s] with id [%s]: incoming version: %d, created at: %v, current version: %d, created at: %v",
		e.SchemaName, e.SchemaID, e.NewVersion, e.NewCreatedAt, e.CurrentVersion, e.CurrentCreatedAt)
}

var (
	ErrRetriable    = errors.New("retriable error")
	ErrInvalidQuery = errors.New("invalid query")

	errNilIDValue      = errors.New("id has nil value")
	errNilVersionValue = errors.New("version has nil value")
	errMetadataMissing = errors.New("missing wal event metadata")
	errEmptyQueueMsg   = errors.New("invalid empty queue message")
	errIncompatibleLSN = errors.New("incompatible LSN value")
)
