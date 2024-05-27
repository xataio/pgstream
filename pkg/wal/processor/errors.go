// SPDX-License-Identifier: Apache-2.0

package processor

import "errors"

var (
	ErrVersionNotFound = errors.New("version column not found")
	ErrIDNotFound      = errors.New("id column not found")
	ErrTableNotFound   = errors.New("table not found")
	ErrColumnNotFound  = errors.New("column not found")
)
