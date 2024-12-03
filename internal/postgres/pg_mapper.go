// SPDX-License-Identifier: Apache-2.0

package postgres

import "github.com/jackc/pgx/v5/pgtype"

type Mapper struct {
	pgMap *pgtype.Map
}

func NewMapper() *Mapper {
	return &Mapper{
		pgMap: pgtype.NewMap(),
	}
}

func (m *Mapper) TypeForOID(oid uint32) string {
	dataType, found := m.pgMap.TypeForOID(oid)
	if !found {
		return ""
	}
	return dataType.Name
}
