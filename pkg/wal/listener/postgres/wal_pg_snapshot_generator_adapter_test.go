// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	generatormocks "github.com/xataio/pgstream/pkg/snapshot/generator/mocks"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestSnapshotGeneratorAdapter_CreateSnapshot(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name         string
		generator    generator.SnapshotGenerator
		schemaTables map[string][]string

		wantErr error
	}{
		{
			name: "ok",
			generator: &generatormocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, &snapshot.Snapshot{
						SchemaName: publicSchema,
						TableNames: []string{"*"},
					}, ss)
					return nil
				},
			},
			schemaTables: map[string][]string{
				publicSchema: {"*"},
			},

			wantErr: nil,
		},
		{
			name: "error",
			generator: &generatormocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					return errTest
				},
			},
			schemaTables: map[string][]string{
				publicSchema: {"*"},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ga := SnapshotGeneratorAdapter{
				logger:          log.NewNoopLogger(),
				generator:       tc.generator,
				processEvent:    func(ctx context.Context, e *wal.Event) error { return nil },
				schemaTables:    tc.schemaTables,
				clock:           clockwork.NewFakeClock(),
				snapshotWorkers: 1,
			}
			defer ga.Close()

			err := ga.CreateSnapshot(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestSnapshotGeneratorAdapter_snapshotRowToWalEvent(t *testing.T) {
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

			ga := SnapshotGeneratorAdapter{
				clock: fakeClock,
			}
			event := ga.snapshotRowToWalEvent(tc.row)

			require.Equal(t, tc.wantEvent, event)
		})
	}
}
