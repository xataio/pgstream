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

type ErrRuleViolation struct {
	Details string
}

func (e *ErrRuleViolation) Error() string {
	return fmt.Sprintf("rule violation: %s", e.Details)
}

type ErrPreconditionFailed struct {
	Details string
}

func (e *ErrPreconditionFailed) Error() string {
	return fmt.Sprintf("precondition failed: %s", e.Details)
}

type ErrCacheLookupFailed struct {
	Details string
}

func (e *ErrCacheLookupFailed) Error() string {
	return fmt.Sprintf("cache lookup failed: %s", e.Details)
}

func MapError(err error) error {
	if pgconn.Timeout(err) {
		return fmt.Errorf("%w: %w", ErrConnTimeout, err)
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42703", "42883", "42P01", "42P02", "42704":
			// 42703 	undefined_column
			// 42883 	undefined_function
			// 42P01 	undefined_table
			// 42P02 	undefined_parameter
			// 42704 	undefined_object
			return &ErrRelationDoesNotExist{
				Details: pgErr.Message,
			}
		case "42601", "42000":
			// 42000 	syntax_error_or_access_rule_violation
			// 42601 	syntax_error
			return &ErrSyntaxError{
				Details: pgErr.Message,
			}
		case "428C9", "42939", "42809":
			// 428C9 	generated_always
			// 42939 	reserved_name
			// 42809 	wrong_object_type
			return &ErrRuleViolation{
				Details: pgErr.Message,
			}
		case "42501":
			// 42501 	insufficient_privilege
			return &ErrPermissionDenied{
				Details: pgErr.Message,
			}
		case "55000":
			// 55000 	object_not_in_prerequisite_state
			return &ErrPreconditionFailed{
				Details: pgErr.Message,
			}
		case "XX000":
			// XX000 	internal_error
			// Only map cache lookup failures to a specific error type for retry logic
			if strings.Contains(pgErr.Message, "cache lookup failed") {
				return &ErrCacheLookupFailed{
					Details: pgErr.Message,
				}
			}
		case "42701", "42P03", "42P04", "42723", "42P05", "42P06", "42P07", "42712", "42710":
			// 42701 	duplicate_column
			// 42P03 	duplicate_cursor
			// 42P04 	duplicate_database
			// 42723 	duplicate_function
			// 42P05 	duplicate_prepared_statement
			// 42P06 	duplicate_schema
			// 42P07 	duplicate_table
			// 42712 	duplicate_alias
			// 42710 	duplicate_object
			return &ErrRelationAlreadyExists{
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
