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

func mapError(err error) error {
	if pgconn.Timeout(err) {
		return ErrConnTimeout
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
		// Class 23 — Integrity Constraint Violation
		if strings.HasPrefix(pgErr.Code, "23") {
			return &ErrConstraintViolation{
				Details: pgErr.Message,
			}
		}
	}

	return err
}
