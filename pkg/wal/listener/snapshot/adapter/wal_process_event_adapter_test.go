// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestProcessEventAdapter_snapshotRowToWalEvent(t *testing.T) {
	t.Parallel()

	now := time.Now()
	fakeClock := clockwork.NewFakeClockAt(now)
	testTable := "table1"
	zeroLSN := "0/0"

	tests := []struct {
		name string
		row  *snapshot.Row

		wantEvent *wal.Event
	}{
		{
			name: "ok - nil row",
			row:  nil,

			wantEvent: nil,
		},
		{
			name: "ok",
			row: &snapshot.Row{
				Schema: publicSchema,
				Table:  testTable,
				Columns: []snapshot.Column{
					{Name: "id", Type: "int4", Value: 1},
					{Name: "name", Type: "text", Value: "alice"},
				},
			},

			wantEvent: &wal.Event{
				CommitPosition: wal.CommitPosition(zeroLSN),
				Data: &wal.Data{
					Action:    "I",
					Timestamp: fakeClock.Now().UTC().Format(time.RFC3339),
					LSN:       zeroLSN,
					Schema:    publicSchema,
					Table:     testTable,
					Columns: []wal.Column{
						{Name: "id", Type: "int4", Value: 1},
						{Name: "name", Type: "text", Value: "alice"},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := ProcessEventAdapter{
				clock: fakeClock,
			}
			event := a.snapshotRowToWalEvent(tc.row)

			require.Equal(t, tc.wantEvent, event)
		})
	}
}
