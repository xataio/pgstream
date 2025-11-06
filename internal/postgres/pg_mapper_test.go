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
				queryRowFn: func(ctx context.Context, query string, args ...any) Row {
					return &mockRow{
						scanFn: func(args ...any) error {
							str, ok := args[0].(*string)
							require.True(t, ok)
							*str = "custom_type"
							return nil
						},
					}
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
				queryRowFn: func(ctx context.Context, query string, args ...any) Row {
					return &mockRow{
						scanFn: func(args ...any) error {
							return errTest
						},
					}
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
