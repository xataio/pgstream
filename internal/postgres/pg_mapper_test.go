// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"

	synclib "github.com/xataio/pgstream/internal/sync"

	"github.com/stretchr/testify/require"
)

func TestMapper_TypeForOID(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name      string
		querier   Querier
		customMap map[uint32]string
		oid       uint32

		wantMap map[uint32]string
		wantErr error
	}{
		{
			name:    "ok - basic type found in pgtype.Map",
			querier: &mockQuerier{},
			oid:     23, // OID for int4

			wantMap: map[uint32]string{},
			wantErr: nil,
		},
		{
			name: "ok - custom type not found in custom map, queried from db",
			querier: &mockQuerier{
				queryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					str, ok := dest[0].(*string)
					require.True(t, ok)
					*str = "custom_type"
					return nil
				},
			},
			oid: 1234,

			wantMap: map[uint32]string{
				1234: "custom_type",
			},
			wantErr: nil,
		},
		{
			name: "ok - custom type found in custom map",
			oid:  1234,
			customMap: map[uint32]string{
				1234: "custom_type",
			},

			wantMap: map[uint32]string{
				1234: "custom_type",
			},
			wantErr: nil,
		},
		{
			name: "error - custom type not found in custom map, error querying from db",
			querier: &mockQuerier{
				queryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
			},
			oid: 1234,

			wantMap: map[uint32]string{},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := NewMapper(tc.querier)

			if tc.customMap != nil {
				m.customOIDMap = synclib.NewMapFromMap(tc.customMap)
			}

			_, err := m.TypeForOID(context.Background(), tc.oid)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantMap, m.customOIDMap.GetMap())
		})
	}
}

func TestMapper_ElementTypeForOID(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	const (
		int4OID        = 23
		int4ArrayOID   = 1007
		citextArrayOID = 20000
		citextOID      = 20001
	)

	tests := []struct {
		name       string
		querier    Querier
		elementMap map[uint32]*ElementType
		oid        uint32

		wantElementType *ElementType
		wantMap         map[uint32]*ElementType
		wantErr         error
	}{
		{
			name:    "ok - built in array resolved from pgtype.Map",
			querier: &mockQuerier{},
			oid:     int4ArrayOID,

			wantElementType: &ElementType{OID: int4OID, Name: "int4"},
			wantMap:         map[uint32]*ElementType{},
			wantErr:         nil,
		},
		{
			name:    "ok - built in scalar is not an array",
			querier: &mockQuerier{},
			oid:     int4OID,

			wantElementType: nil,
			wantMap:         map[uint32]*ElementType{},
			wantErr:         nil,
		},
		{
			name: "ok - user defined array queried from db",
			querier: &mockQuerier{
				queryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					oid, ok := dest[0].(*uint32)
					require.True(t, ok)
					name, ok := dest[1].(*string)
					require.True(t, ok)
					*oid, *name = citextOID, "citext"
					return nil
				},
			},
			oid: citextArrayOID,

			wantElementType: &ElementType{OID: citextOID, Name: "citext"},
			wantMap:         map[uint32]*ElementType{citextArrayOID: {OID: citextOID, Name: "citext"}},
			wantErr:         nil,
		},
		{
			name: "ok - user defined array found in cache",
			oid:  citextArrayOID,
			elementMap: map[uint32]*ElementType{
				citextArrayOID: {OID: citextOID, Name: "citext"},
			},

			wantElementType: &ElementType{OID: citextOID, Name: "citext"},
			wantMap:         map[uint32]*ElementType{citextArrayOID: {OID: citextOID, Name: "citext"}},
			wantErr:         nil,
		},
		{
			name: "ok - user defined scalar caches the negative answer",
			querier: &mockQuerier{
				queryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return ErrNoRows
				},
			},
			oid: citextOID,

			wantElementType: nil,
			wantMap:         map[uint32]*ElementType{citextOID: nil},
			wantErr:         nil,
		},
		{
			name: "error - querying the element type",
			querier: &mockQuerier{
				queryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					return errTest
				},
			},
			oid: citextArrayOID,

			wantElementType: nil,
			wantMap:         map[uint32]*ElementType{},
			wantErr:         errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := NewMapper(tc.querier)

			if tc.elementMap != nil {
				m.elementOIDMap = synclib.NewMapFromMap(tc.elementMap)
			}

			elementType, err := m.ElementTypeForOID(context.Background(), tc.oid)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantElementType, elementType)
			require.Equal(t, tc.wantMap, m.elementOIDMap.GetMap())
		})
	}
}
