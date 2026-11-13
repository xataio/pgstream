// SPDX-License-Identifier: Apache-2.0

package transaction

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

const testGeneration uint64 = 0

func TestTrackerWaitsForEveryTransactionEntry(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	require.NoError(t, tracker.Begin(1, testGeneration))
	entry1, err := tracker.TrackEntry(1, testGeneration)
	require.NoError(t, err)
	entry2, err := tracker.TrackEntry(1, testGeneration)
	require.NoError(t, err)
	require.NoError(t, tracker.Commit(context.Background(), 1, testGeneration, "0/100"))

	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{entry1}))
	require.Empty(t, checkpoints)

	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{entry2}))
	require.Equal(t, [][]wal.CommitPosition{{"0/100"}}, checkpoints)
}

func TestTrackerAdvancesOnlyContiguousTransactions(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	require.NoError(t, tracker.Begin(1, testGeneration))
	tx1Entry, err := tracker.TrackEntry(1, testGeneration)
	require.NoError(t, err)
	require.NoError(t, tracker.Commit(context.Background(), 1, testGeneration, "0/100"))

	require.NoError(t, tracker.Begin(2, testGeneration))
	tx2Entry, err := tracker.TrackEntry(2, testGeneration)
	require.NoError(t, err)
	require.NoError(t, tracker.Commit(context.Background(), 2, testGeneration, "0/200"))

	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{tx2Entry}))
	require.Empty(t, checkpoints)

	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{tx1Entry}))
	require.Equal(t, [][]wal.CommitPosition{{"0/200"}}, checkpoints)
}

func TestTrackerHandlesAcknowledgementBeforeCommit(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	require.NoError(t, tracker.Begin(1, testGeneration))
	entry, err := tracker.TrackEntry(1, testGeneration)
	require.NoError(t, err)
	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{entry}))
	require.Empty(t, checkpoints)

	require.NoError(t, tracker.Commit(context.Background(), 1, testGeneration, "0/100"))
	require.Equal(t, [][]wal.CommitPosition{{"0/100"}}, checkpoints)
}

func TestTrackerKeepsStateWhenSourceCheckpointFails(t *testing.T) {
	errTest := errors.New("checkpoint failed")
	calls := 0
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		calls++
		if calls == 1 {
			return errTest
		}
		require.Equal(t, []wal.CommitPosition{"0/100"}, positions)
		return nil
	})

	require.NoError(t, tracker.Begin(1, testGeneration))
	entry, err := tracker.TrackEntry(1, testGeneration)
	require.NoError(t, err)
	require.NoError(t, tracker.Commit(context.Background(), 1, testGeneration, "0/100"))
	require.ErrorIs(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{entry}), errTest)

	// A later checkpoint attempt retries the same completed safe prefix.
	require.NoError(t, tracker.Checkpoint(context.Background(), nil))
	require.Equal(t, 2, calls)
}

func TestTrackerDoesNotLetKeepalivePassOpenTransaction(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	require.NoError(t, tracker.Begin(1, testGeneration))
	entry, err := tracker.TrackEntry(1, testGeneration)
	require.NoError(t, err)
	keepalive := tracker.TrackKeepalive(testGeneration)
	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{keepalive}))
	require.Empty(t, checkpoints)

	require.NoError(t, tracker.Commit(context.Background(), 1, testGeneration, "0/100"))
	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{entry}))
	require.Equal(t, [][]wal.CommitPosition{{"0/100"}}, checkpoints)
}

func TestTrackerRepliesToIdleKeepaliveWithoutAdvancingToServerWALEnd(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	keepalive := tracker.TrackKeepalive(testGeneration)
	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{keepalive}))
	require.Equal(t, [][]wal.CommitPosition{{wal.ZeroLSN}}, checkpoints)
	require.Empty(t, tracker.queue)
}

func TestTrackerReleasesConsecutiveIdleKeepalives(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	keepalives := []wal.CommitPosition{
		tracker.TrackKeepalive(testGeneration),
		tracker.TrackKeepalive(testGeneration),
		tracker.TrackKeepalive(testGeneration),
	}
	require.Len(t, tracker.queue, len(keepalives))

	require.NoError(t, tracker.Checkpoint(context.Background(), keepalives))
	require.Equal(t, [][]wal.CommitPosition{{wal.ZeroLSN}}, checkpoints)
	require.Empty(t, tracker.queue)
}

func TestTrackerReleasesTransactionAndFollowingKeepalive(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	require.NoError(t, tracker.Begin(1, testGeneration))
	entry, err := tracker.TrackEntry(1, testGeneration)
	require.NoError(t, err)
	require.NoError(t, tracker.Commit(context.Background(), 1, testGeneration, "0/100"))
	keepalive := tracker.TrackKeepalive(testGeneration)

	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{entry, keepalive}))
	require.Equal(t, [][]wal.CommitPosition{{"0/100"}}, checkpoints)
	require.Empty(t, tracker.queue)
	require.Empty(t, tracker.byTxID)
}

func TestTrackerRetriesFinalPositionForCompletedPrefix(t *testing.T) {
	errTest := errors.New("checkpoint failed")
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		if len(checkpoints) == 1 {
			return errTest
		}
		return nil
	})

	require.NoError(t, tracker.Begin(1, testGeneration))
	tx1Entry, err := tracker.TrackEntry(1, testGeneration)
	require.NoError(t, err)
	require.NoError(t, tracker.Commit(context.Background(), 1, testGeneration, "0/100"))

	require.NoError(t, tracker.Begin(2, testGeneration))
	tx2Entry, err := tracker.TrackEntry(2, testGeneration)
	require.NoError(t, err)
	require.NoError(t, tracker.Commit(context.Background(), 2, testGeneration, "0/200"))

	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{tx2Entry}))
	require.ErrorIs(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{tx1Entry}), errTest)
	require.Len(t, tracker.queue, 2)

	require.NoError(t, tracker.Checkpoint(context.Background(), nil))
	require.Equal(t, [][]wal.CommitPosition{{"0/200"}, {"0/200"}}, checkpoints)
	require.Empty(t, tracker.queue)
}

func TestTrackerStartsNewGenerationAndIgnoresStaleAcknowledgements(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	require.NoError(t, tracker.Begin(1, 0))
	staleEntry, err := tracker.TrackEntry(1, 0)
	require.NoError(t, err)

	// PostgreSQL reconnects and replays the transaction. Local transaction IDs
	// can be reused because the generation distinguishes the new stream.
	require.NoError(t, tracker.Begin(1, 1))
	currentEntry, err := tracker.TrackEntry(1, 1)
	require.NoError(t, err)
	require.NoError(t, tracker.Commit(context.Background(), 1, 1, "0/200"))

	// One target batch may contain work queued by both connections. The stale
	// token is ignored while the current token still completes the transaction.
	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{staleEntry, currentEntry}))
	require.Equal(t, [][]wal.CommitPosition{{"0/200"}}, checkpoints)
	require.Empty(t, tracker.queue)
	require.Empty(t, tracker.byToken)
}

func TestTrackerKeepaliveCanStartNewGeneration(t *testing.T) {
	var checkpoints [][]wal.CommitPosition
	tracker := NewTracker(func(_ context.Context, positions []wal.CommitPosition) error {
		checkpoints = append(checkpoints, append([]wal.CommitPosition(nil), positions...))
		return nil
	})

	require.NoError(t, tracker.Begin(1, 0))
	staleEntry, err := tracker.TrackEntry(1, 0)
	require.NoError(t, err)

	keepalive := tracker.TrackKeepalive(1)
	require.NoError(t, tracker.Checkpoint(context.Background(), []wal.CommitPosition{staleEntry, keepalive}))
	require.Equal(t, [][]wal.CommitPosition{{wal.ZeroLSN}}, checkpoints)
	require.Empty(t, tracker.queue)
}

func TestTrackerRejectsMalformedAcknowledgementToken(t *testing.T) {
	tracker := NewTracker(nil)
	err := tracker.Checkpoint(context.Background(), []wal.CommitPosition{"not-a-token"})
	require.ErrorIs(t, err, errUnknownAckToken)
}
