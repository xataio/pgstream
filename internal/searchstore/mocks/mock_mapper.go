// SPDX-License-Identifier: Apache-2.0

package mocks

import "github.com/xataio/pgstream/internal/searchstore"

type Mapper struct {
	GetDefaultIndexSettingsFn func() map[string]any
	FieldMappingFn            func(*searchstore.Field) (map[string]any, error)
}

func (m *Mapper) GetDefaultIndexSettings() map[string]any {
	if m.GetDefaultIndexSettingsFn == nil {
		return map[string]any{}
	}
	return m.GetDefaultIndexSettingsFn()
}

func (m *Mapper) FieldMapping(f *searchstore.Field) (map[string]any, error) {
	return m.FieldMappingFn(f)
}
