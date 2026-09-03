// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
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
	// enumOIDMap caches whether an OID resolves to an enum, and if so its
	// labels. A nil value is a cached negative answer, which most OIDs are.
	enumOIDMap *synclib.Map[uint32, *EnumType]
}

// EnumType describes a column type that names a user-defined enum, either
// directly or through a domain over one.
type EnumType struct {
	// Name is the enum's own type name, which is not always the column's type
	// name: a domain resolves to the enum underneath it.
	Name string
	// Labels are the enum's values, in their declared sort order. They are
	// shared with every caller for this OID and must not be modified.
	Labels []string
}

// NewMapper creates a new Mapper instance with the given database querier.
// The mapper is initialized with the standard pgx type map and an empty
// custom type cache.
func NewMapper(conn Querier) *Mapper {
	return &Mapper{
		querier:      conn,
		pgMap:        pgtype.NewMap(),
		customOIDMap: synclib.NewMap[uint32, string](),
		enumOIDMap:   synclib.NewMap[uint32, *EnumType](),
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
	if err := m.querier.QueryRow(ctx, []any{&dataType}, "SELECT typname FROM pg_type WHERE oid = $1", oid); err != nil {
		return "unknown", fmt.Errorf("selecting type for OID %d: %w", oid, err)
	}

	m.customOIDMap.Set(oid, dataType)
	return dataType, nil
}

// enumTypeQuery resolves an OID to the enum it names, together with the enum's
// labels in their declared order. It returns no row for anything else.
//
// Postgres reports a domain column's base type in the row description, so a
// domain over an enum arrives here as the enum itself. An array of an enum has
// its own OID, whose typtype is 'b', so it matches nothing here and is left to
// the caller's type check to reject by name like any other array.
const enumTypeQuery = `SELECT t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder)
	FROM pg_type t
	JOIN pg_enum e ON e.enumtypid = t.oid
	WHERE t.oid = $1 AND t.typtype = 'e'
	GROUP BY t.typname`

// EnumForOID returns the enum the given OID names, or nil when it names none.
// Built-in types are answered from the pgx type map without a query, since no
// enum can have a built-in OID.
//
// The returned value is shared with every other caller for that OID and must
// not be modified; clone Labels before handing them to anything that might.
// Note: This method may acquire a database connection if the OID is not cached.
func (m *Mapper) EnumForOID(ctx context.Context, oid uint32) (*EnumType, error) {
	if _, isBuiltIn := m.pgMap.TypeForOID(oid); isBuiltIn {
		return nil, nil
	}
	if enum, found := m.enumOIDMap.Get(oid); found {
		return enum, nil
	}

	var enum EnumType
	err := m.querier.QueryRow(ctx, []any{&enum.Name, &enum.Labels}, enumTypeQuery, oid)
	switch {
	case err == nil:
		m.enumOIDMap.Set(oid, &enum)
		return &enum, nil
	case errors.Is(err, ErrNoRows):
		m.enumOIDMap.Set(oid, nil)
		return nil, nil
	default:
		return nil, fmt.Errorf("selecting enum for OID %d: %w", oid, err)
	}
}
