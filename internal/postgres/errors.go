// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrConnTimeout = errors.New("connection timeout")
	ErrNoRows      = errors.New("no rows")
)

func mapError(err error) error {
	if pgconn.Timeout(err) {
		return ErrConnTimeout
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	}

	return err
}
