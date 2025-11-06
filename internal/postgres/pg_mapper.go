// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	synclib "github.com/xataio/pgstream/internal/sync"

	"github.com/jackc/pgx/v5/pgtype"
)

type Mapper struct {
	querier      Querier
	pgMap        *pgtype.Map
	customOIDMap *synclib.Map[uint32, string]
}

func NewMapper(conn Querier) *Mapper {
	return &Mapper{
		querier:      conn,
		pgMap:        pgtype.NewMap(),
		customOIDMap: synclib.NewMap[uint32, string](),
	}
}

func (m *Mapper) TypeForOID(ctx context.Context, oid uint32) (string, error) {
	dataType, found := m.pgMap.TypeForOID(oid)
	if !found {
		return m.queryType(ctx, oid)
	}
	return dataType.Name, nil
}

func (m *Mapper) queryType(ctx context.Context, oid uint32) (string, error) {
	if customType, found := m.customOIDMap.Get(oid); found {
		return customType, nil
	}

	var dataType string
	if err := m.querier.QueryRow(ctx, "SELECT typname FROM pg_type WHERE oid = $1", oid).Scan(&dataType); err != nil {
		return "unknown", fmt.Errorf("selecting type for OID %d: %w", oid, err)
	}

	m.customOIDMap.Set(oid, dataType)
	return dataType, nil
}
