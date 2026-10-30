// SPDX-License-Identifier: Apache-2.0

package transaction

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

type testProcessor struct {
	events []*wal.Event
}

func (p *testProcessor) ProcessWALEvent(_ context.Context, event *wal.Event) error {
	p.events = append(p.events, event)
	return nil
}

func (p *testProcessor) Name() string { return "test" }
func (p *testProcessor) Close() error { return nil }

func TestProcessorTracksTargetEntriesAndConsumesBoundaries(t *testing.T) {
	var checkpointed []wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpointed = append(checkpointed, positions...)
		return nil
	})
	target := &testProcessor{}
	processor := NewProcessor(target, tracker)
	ctx := context.Background()

	require.NoError(t, processor.ProcessWALEvent(ctx, &wal.Event{
		Transaction: &wal.Transaction{ID: 1, Boundary: wal.TransactionBegin},
	}))
	require.NoError(t, processor.ProcessWALEvent(ctx, &wal.Event{
		Data:           &wal.Data{Action: "I"},
		CommitPosition: "0/80",
		Transaction:    &wal.Transaction{ID: 1, Boundary: wal.TransactionEntry},
	}))
	require.NoError(t, processor.ProcessWALEvent(ctx, &wal.Event{
		CommitPosition: "0/100",
		Transaction:    &wal.Transaction{ID: 1, Boundary: wal.TransactionCommit},
	}))

	require.Len(t, target.events, 1)
	require.Equal(t, wal.CommitPosition("0/80"), target.events[0].CommitPosition)
	token := target.events[0].CheckpointPosition
	require.NotEmpty(t, token)
	require.NotEqual(t, target.events[0].CommitPosition, token)
	require.Empty(t, checkpointed)

	require.NoError(t, tracker.Checkpoint(ctx, []wal.CommitPosition{token}))
	require.Equal(t, []wal.CommitPosition{"0/100"}, checkpointed)
}

func TestProcessorDoesNotTrackResolvedNilEntry(t *testing.T) {
	var checkpointed []wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpointed = append(checkpointed, positions...)
		return nil
	})
	target := &testProcessor{}
	processor := NewProcessor(target, tracker)
	ctx := context.Background()

	require.NoError(t, processor.ProcessWALEvent(ctx, &wal.Event{
		Transaction: &wal.Transaction{ID: 1, Boundary: wal.TransactionBegin},
	}))
	require.NoError(t, processor.ProcessWALEvent(ctx, &wal.Event{
		CommitPosition: "0/80",
		Transaction:    &wal.Transaction{ID: 1, Boundary: wal.TransactionEntry},
	}))
	require.NoError(t, processor.ProcessWALEvent(ctx, &wal.Event{
		CommitPosition: "0/100",
		Transaction:    &wal.Transaction{ID: 1, Boundary: wal.TransactionCommit},
	}))

	require.Empty(t, target.events)
	require.Equal(t, []wal.CommitPosition{"0/100"}, checkpointed)
}
