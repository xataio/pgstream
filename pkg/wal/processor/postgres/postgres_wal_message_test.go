// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestWalMessage_IsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  *walMessage
		want bool
	}{
		{
			name: "nil message",
			msg:  nil,
			want: true,
		},
		{
			name: "nil data",
			msg:  &walMessage{},
			want: true,
		},
		{
			name: "non-empty",
			msg: &walMessage{
				data: &wal.Data{Action: "I", Schema: "public", Table: "t"},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.msg.IsEmpty())
		})
	}
}

func TestWalMessage_Size(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  *walMessage
		want int
	}{
		{
			name: "nil message",
			msg:  nil,
			want: 0,
		},
		{
			name: "nil data",
			msg:  &walMessage{},
			want: 0,
		},
		{
			name: "non-empty - size is positive",
			msg: &walMessage{
				data: &wal.Data{
					Action: "I",
					Schema: "public",
					Table:  "users",
					Columns: []wal.Column{
						{Name: "id", Type: "bigint", Value: float64(1)},
						{Name: "name", Type: "text", Value: "alice"},
					},
				},
			},
			want: -1, // just check > 0
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			size := tc.msg.Size()
			if tc.want == -1 {
				require.Greater(t, size, 0)
			} else {
				require.Equal(t, tc.want, size)
			}
		})
	}
}
