// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"

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
	}

	return err
}
