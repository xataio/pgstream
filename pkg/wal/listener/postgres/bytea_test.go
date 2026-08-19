// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

func Test_decodeByteaColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data *wal.Data

		want *wal.Data
	}{
		{
			name: "nil data",
			data: nil,
			want: nil,
		},
		{
			name: "wal2json bare hex, in columns and identity",
			data: &wal.Data{
				Columns:  []wal.Column{{Name: "payload", Type: "bytea", Value: "deadbeef"}},
				Identity: []wal.Column{{Name: "key", Type: "bytea", Value: "0102"}},
			},
			want: &wal.Data{
				Columns:  []wal.Column{{Name: "payload", Type: "bytea", Value: []byte{0xde, 0xad, 0xbe, 0xef}}},
				Identity: []wal.Column{{Name: "key", Type: "bytea", Value: []byte{0x01, 0x02}}},
			},
		},
		{
			name: "postgres hex format",
			data: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: `\xdeadbeef`}}},
			want: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: []byte{0xde, 0xad, 0xbe, 0xef}}}},
		},
		{
			name: "empty value",
			data: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: ""}}},
			want: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: []byte{}}}},
		},
		{
			// snapshots hand bytea over as []byte already
			name: "already decoded",
			data: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: []byte{0x01}}}},
			want: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: []byte{0x01}}}},
		},
		{
			// degrade to the previous behaviour rather than failing the batch
			name: "not valid hex",
			data: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: "not-hex"}}},
			want: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: "not-hex"}}},
		},
		{
			name: "odd length hex",
			data: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: "abc"}}},
			want: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: "abc"}}},
		},
		{
			name: "nil value",
			data: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: nil}}},
			want: &wal.Data{Columns: []wal.Column{{Name: "payload", Type: "bytea", Value: nil}}},
		},
		{
			name: "other column types are untouched",
			data: &wal.Data{Columns: []wal.Column{
				{Name: "name", Type: "text", Value: "deadbeef"},
				{Name: "id", Type: "integer", Value: 1},
			}},
			want: &wal.Data{Columns: []wal.Column{
				{Name: "name", Type: "text", Value: "deadbeef"},
				{Name: "id", Type: "integer", Value: 1},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			decodeByteaColumns(tc.data)
			require.Equal(t, tc.want, tc.data)
		})
	}
}
