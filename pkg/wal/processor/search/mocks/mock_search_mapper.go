// SPDX-License-Identifier: Apache-2.0

package mocks

import "github.com/xataio/pgstream/pkg/schemalog"

type Mapper struct {
	ColumnToSearchMappingFn func(column schemalog.Column) (map[string]any, error)
	MapColumnValueFn        func(column schemalog.Column, value any) (any, error)
}

func (m *Mapper) ColumnToSearchMapping(column schemalog.Column) (map[string]any, error) {
	return m.ColumnToSearchMappingFn(column)
}

func (m *Mapper) MapColumnValue(column schemalog.Column, value any) (any, error) {
	return m.MapColumnValueFn(column, value)
}
