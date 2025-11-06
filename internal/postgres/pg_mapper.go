// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	synclib "github.com/xataio/pgstream/internal/sync"

	"github.com/jackc/pgx/v5/pgtype"
)

// Mapper provides PostgreSQL type information mapping from OIDs to type names.
// It uses a combination of the pgx type map for standard types and a custom
// cache for user-defined types, querying the database when necessary.
type Mapper struct {
	// querier is used to execute queries against PostgreSQL when type information
	// is not available in the caches
	querier Querier
	// pgMap contains the standard PostgreSQL type mappings from the pgx library
	pgMap *pgtype.Map
	// customOIDMap is a thread-safe cache for custom OID to type name mappings
	// that are queried from pg_type. This prevents repeated database queries
	// for the same custom types.
	customOIDMap *synclib.Map[uint32, string]
}

// NewMapper creates a new Mapper instance with the given database querier.
// The mapper is initialized with the standard pgx type map and an empty
// custom type cache.
func NewMapper(conn Querier) *Mapper {
	return &Mapper{
		querier:      conn,
		pgMap:        pgtype.NewMap(),
		customOIDMap: synclib.NewMap[uint32, string](),
	}
}

// TypeForOID returns the PostgreSQL type name for the given OID.
// It first checks the standard pgx type map, then the custom type cache,
// and finally queries the database if the type is not found in either cache.
// Note: This method may acquire a database connection if the type is not cached.
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
