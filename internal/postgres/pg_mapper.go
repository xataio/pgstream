// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

type Mapper struct {
	querier Querier
	pgMap   *pgtype.Map
}

func NewMapper(conn Querier) *Mapper {
	return &Mapper{
		querier: conn,
		pgMap:   pgtype.NewMap(),
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
	var dataType string
	if err := m.querier.QueryRow(ctx, "SELECT typname FROM pg_type WHERE oid = $1", oid).Scan(&dataType); err != nil {
		return "unknown", fmt.Errorf("selecting type for OID %d: %w", oid, err)
	}
	return dataType, nil
}
