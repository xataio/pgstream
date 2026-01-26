// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"github.com/xataio/pgstream/pkg/wal"
)

type Mapper struct {
	ColumnToSearchMappingFn func(column *wal.DDLColumn) (map[string]any, error)
	MapColumnValueFn        func(column *wal.Column) (any, error)
}

func (m *Mapper) ColumnToSearchMapping(column *wal.DDLColumn) (map[string]any, error) {
	return m.ColumnToSearchMappingFn(column)
}

func (m *Mapper) MapColumnValue(column *wal.Column) (any, error) {
	return m.MapColumnValueFn(column)
}
