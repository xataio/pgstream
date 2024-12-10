// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/snapshot"
)

func TestAggregator_CreateSnapshot(t *testing.T) {
	t.Parallel()

	testSnapshot := &snapshot.Snapshot{
		SchemaName: "test-schema",
		TableNames: []string{"table1", "table2"},
	}

	newGenerator := func(err error) *mockGenerator {
		return &mockGenerator{
			createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
				require.Equal(t, testSnapshot, ss)
				return err
			},
		}
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name       string
		generators []SnapshotGenerator

		wantErr error
	}{
		{
			name:       "ok",
			generators: []SnapshotGenerator{newGenerator(nil), newGenerator(nil)},
			wantErr:    nil,
		},
		{
			name:       "error on first generator",
			generators: []SnapshotGenerator{newGenerator(errTest), newGenerator(errors.New("should not be called"))},
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			agg := NewAggregator(tc.generators)

			err := agg.CreateSnapshot(context.Background(), testSnapshot)
			require.Equal(t, err, tc.wantErr)
		})
	}
}

func TestAggregator_Close(t *testing.T) {
	t.Parallel()

	newGenerator := func(err error) *mockGenerator {
		return &mockGenerator{
			closeFn: func() error {
				return err
			},
		}
	}

	errTest1 := errors.New("oh noes")
	errTest2 := errors.New("oh nose")

	tests := []struct {
		name       string
		generators []SnapshotGenerator

		wantErr error
	}{
		{
			name:       "ok",
			generators: []SnapshotGenerator{newGenerator(nil), newGenerator(nil)},
			wantErr:    nil,
		},
		{
			name:       "error on first generator",
			generators: []SnapshotGenerator{newGenerator(errTest1), newGenerator(errTest2)},
			wantErr:    errors.Join(errTest1, errTest2),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			agg := NewAggregator(tc.generators)

			err := agg.Close()
			require.Equal(t, err, tc.wantErr)
		})
	}
}
