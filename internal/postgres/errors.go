// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrConnTimeout = errors.New("connection timeout")
	ErrNoRows      = errors.New("no rows")
)

type ErrRelationDoesNotExist struct {
	Details string
}

func (e *ErrRelationDoesNotExist) Error() string {
	return fmt.Sprintf("relation does not exist: %s", e.Details)
}

type ErrConstraintViolation struct {
	Details string
}

func (e *ErrConstraintViolation) Error() string {
	return fmt.Sprintf("constraint violation: %s", e.Details)
}

type ErrPermissionDenied struct {
	Details string
}

func (e *ErrPermissionDenied) Error() string {
	return fmt.Sprintf("permission denied: %s", e.Details)
}

type ErrSyntaxError struct {
	Details string
}

func (e *ErrSyntaxError) Error() string {
	return fmt.Sprintf("syntax error: %s", e.Details)
}

type ErrDataException struct {
	Details string
}

func (e *ErrDataException) Error() string {
	return fmt.Sprintf("data exception: %s", e.Details)
}

type ErrRelationAlreadyExists struct {
	Details string
}

func (e *ErrRelationAlreadyExists) Error() string {
	return fmt.Sprintf("relation already exists: %v", e.Details)
}

func mapError(err error) error {
	if pgconn.Timeout(err) {
		return fmt.Errorf("%w: %w", ErrConnTimeout, err)
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "42P01" {
			return &ErrRelationDoesNotExist{
				Details: pgErr.Message,
			}
		}
		if pgErr.Code == "42601" {
			return &ErrSyntaxError{
				Details: pgErr.Message,
			}
		}

		if pgErr.Code == "42501" {
			return &ErrPermissionDenied{
				Details: pgErr.Message,
			}
		}

		// Class 22 — Data Exception
		if strings.HasPrefix(pgErr.Code, "22") {
			return &ErrDataException{
				Details: pgErr.Message,
			}
		}
		// Class 23 — Integrity Constraint Violation
		if strings.HasPrefix(pgErr.Code, "23") {
			return &ErrConstraintViolation{
				Details: pgErr.Message,
			}
		}
	}

	return err
}
