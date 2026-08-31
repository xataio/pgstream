// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/phase"
)

type mockGenerator struct {
	createSnapshotFn func(context.Context) error
	closeFn          func() error
}

func (m *mockGenerator) CreateSnapshot(ctx context.Context) error {
	if m.createSnapshotFn != nil {
		return m.createSnapshotFn(ctx)
	}
	return nil
}

func (m *mockGenerator) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

// TestListener_Listen_setsSnapshotPhase pins the phase the /status endpoint
// reports while a snapshot-only run is in progress. The tracker has to be set
// before the generator is handed control, or a long snapshot reports an empty
// phase for its whole duration.
func TestListener_Listen_setsSnapshotPhase(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name             string
		createSnapshotFn func(context.Context) error

		wantErr error
	}{
		{
			name: "ok",
			createSnapshotFn: func(context.Context) error {
				return nil
			},

			wantErr: nil,
		},
		{
			name: "error - the phase is still set before the generator runs",
			createSnapshotFn: func(context.Context) error {
				return errTest
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tracker := phase.NewTracker()
			require.Equal(t, phase.Phase(""), tracker.Get())

			var phaseOnEntry phase.Phase
			generator := &mockGenerator{
				createSnapshotFn: func(ctx context.Context) error {
					phaseOnEntry = tracker.Get()
					return tc.createSnapshotFn(ctx)
				},
			}

			listener := New(generator, WithPhaseTracker(tracker))

			err := listener.Listen(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, phase.Snapshot, phaseOnEntry,
				"the phase must be set before the generator is invoked")
			require.Equal(t, phase.Snapshot, tracker.Get())
		})
	}
}

func TestListener_Listen_nilPhaseTracker(t *testing.T) {
	t.Parallel()

	called := false
	listener := New(&mockGenerator{
		createSnapshotFn: func(context.Context) error {
			called = true
			return nil
		},
	})

	require.NoError(t, listener.Listen(context.Background()))
	require.True(t, called)
}

func TestListener_Close(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	listener := New(&mockGenerator{
		closeFn: func() error { return errTest },
	})

	require.ErrorIs(t, listener.Close(), errTest)
}
