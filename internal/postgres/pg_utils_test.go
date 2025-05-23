// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func Test_newIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string

		wantIdentifier pgx.Identifier
		wantErr        error
	}{
		{
			name:      "ok - table name",
			tableName: "test_table",

			wantIdentifier: pgx.Identifier{"test_table"},
			wantErr:        nil,
		},
		{
			name:      "ok - qualified table name",
			tableName: "test_schema.test_table",

			wantIdentifier: pgx.Identifier{"test_schema", "test_table"},
			wantErr:        nil,
		},
		{
			name:      "ok - quoted qualified table name",
			tableName: `"test_schema"."test_table"`,

			wantIdentifier: pgx.Identifier{"test_schema", "test_table"},
			wantErr:        nil,
		},
		{
			name:      "error - invalid table name",
			tableName: "invalid.test.table",

			wantIdentifier: nil,
			wantErr:        errors.New("invalid table name: invalid.test.table"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id, err := newIdentifier(tc.tableName)
			require.Equal(t, tc.wantErr, err)
			require.Equal(t, tc.wantIdentifier, id)
		})
	}
}
