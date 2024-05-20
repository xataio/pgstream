// SPDX-License-Identifier: Apache-2.0

package search

import (
	"fmt"

	"github.com/xataio/pgstream/pkg/schemalog"
)

type Mapper interface {
	ColumnToSearchMapping(column schemalog.Column) (map[string]any, error)
	MapColumnValue(column schemalog.Column, value any) (any, error)
}

type ErrTypeInvalid struct {
	Input string
}

func (e ErrTypeInvalid) Error() string {
	return fmt.Sprintf("unsupported type: %s", e.Input)
}
