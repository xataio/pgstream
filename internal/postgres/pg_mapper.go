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
	// elementOIDMap is a thread-safe cache for the element type of array OIDs
	// the pgx map does not know. A nil value caches the answer that the OID
	// does not name an array, so a scalar user defined type is queried once.
	elementOIDMap *synclib.Map[uint32, *ElementType]
}

// ElementType identifies the element type of a postgres array type.
type ElementType struct {
	OID  uint32
	Name string
}

// NewMapper creates a new Mapper instance with the given database querier.
// The mapper is initialized with the standard pgx type map and an empty
// custom type cache.
func NewMapper(conn Querier) *Mapper {
	return &Mapper{
		querier:       conn,
		pgMap:         pgtype.NewMap(),
		customOIDMap:  synclib.NewMap[uint32, string](),
		elementOIDMap: synclib.NewMap[uint32, *ElementType](),
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

const elementTypeQuery = `SELECT e.oid, e.typname
	FROM pg_type a JOIN pg_type e ON e.oid = a.typelem
	WHERE a.oid = $1 AND a.typcategory = 'A'`

// ElementTypeForOID returns the element type of the array type named by oid,
// or nil if the OID does not name an array.
func (m *Mapper) ElementTypeForOID(ctx context.Context, oid uint32) (*ElementType, error) {
	if dataType, found := m.pgMap.TypeForOID(oid); found {
		arrayCodec, isArray := dataType.Codec.(*pgtype.ArrayCodec)
		if !isArray {
			return nil, nil
		}
		return &ElementType{OID: arrayCodec.ElementType.OID, Name: arrayCodec.ElementType.Name}, nil
	}
	return m.queryElementType(ctx, oid)
}

func (m *Mapper) queryElementType(ctx context.Context, oid uint32) (*ElementType, error) {
	if elementType, found := m.elementOIDMap.Get(oid); found {
		return elementType, nil
	}

	var elementOID uint32
	var elementName string
	if err := m.querier.QueryRow(ctx, []any{&elementOID, &elementName}, elementTypeQuery, oid); err != nil {
		if !errors.Is(err, ErrNoRows) {
			return nil, fmt.Errorf("selecting element type for OID %d: %w", oid, err)
		}
		// not an array; cache the negative so the catalog is queried once
		m.elementOIDMap.Set(oid, nil)
		return nil, nil
	}

	elementType := &ElementType{OID: elementOID, Name: elementName}
	m.elementOIDMap.Set(oid, elementType)
	return elementType, nil
}
