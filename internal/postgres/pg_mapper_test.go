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

func TestMapper_EnumForOID(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	// each querier counts its calls, so the cache can be asserted on rather
	// than only its contents
	newQuerier := func(name string, labels []string, err error, calls *int) Querier {
		return &mockQuerier{
			queryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
				*calls++
				if err != nil {
					return err
				}
				require.Equal(t, enumTypeQuery, query)
				require.Len(t, dest, 2)
				gotName, ok := dest[0].(*string)
				require.True(t, ok)
				*gotName = name
				gotLabels, ok := dest[1].(*[]string)
				require.True(t, ok)
				*gotLabels = labels
				return nil
			},
		}
	}

	tests := []struct {
		name string
		// querier is built per case so the call count can be shared with it
		newQuerier func(calls *int) Querier
		oid        uint32

		wantEnum  *EnumType
		wantCache map[uint32]*EnumType
		wantCalls int
		wantErr   error
	}{
		{
			name: "built-in type is answered without a query",
			newQuerier: func(calls *int) Querier {
				return &mockQuerier{queryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
					*calls++
					t.Errorf("unexpected query %q for a built-in OID", query)
					return nil
				}}
			},
			oid: 23, // int4

			wantEnum:  nil,
			wantCache: map[uint32]*EnumType{},
			wantCalls: 0,
		},
		{
			name: "enum resolved, then served from the cache",
			newQuerier: func(calls *int) Querier {
				return newQuerier("mood", []string{"sad", "ok", "happy"}, nil, calls)
			},
			oid: 16385,

			wantEnum: &EnumType{Name: "mood", Labels: []string{"sad", "ok", "happy"}},
			wantCache: map[uint32]*EnumType{
				16385: {Name: "mood", Labels: []string{"sad", "ok", "happy"}},
			},
			wantCalls: 1,
		},
		{
			name: "a custom type that is no enum caches the negative answer",
			newQuerier: func(calls *int) Querier {
				return newQuerier("", nil, ErrNoRows, calls)
			},
			oid: 1234,

			wantEnum:  nil,
			wantCache: map[uint32]*EnumType{1234: nil},
			wantCalls: 1,
		},
		{
			name: "an error is not cached, so the next call retries",
			newQuerier: func(calls *int) Querier {
				return newQuerier("", nil, errTest, calls)
			},
			oid: 1234,

			wantEnum:  nil,
			wantCache: map[uint32]*EnumType{},
			wantCalls: 2,
			wantErr:   errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			m := NewMapper(tc.newQuerier(&calls))

			enum, err := m.EnumForOID(context.Background(), tc.oid)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantEnum, enum)

			// a second lookup must not re-query unless the first failed
			enumAgain, errAgain := m.EnumForOID(context.Background(), tc.oid)
			require.ErrorIs(t, errAgain, tc.wantErr)
			require.Equal(t, tc.wantEnum, enumAgain)

			require.Equal(t, tc.wantCalls, calls)
			require.Equal(t, tc.wantCache, m.enumOIDMap.GetMap())
		})
	}
}
