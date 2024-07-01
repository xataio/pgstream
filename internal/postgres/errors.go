// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

var ErrConnTimeout = errors.New("connection timeout")

func mapError(err error) error {
	if pgconn.Timeout(err) {
		return ErrConnTimeout
	}

	return err
}
