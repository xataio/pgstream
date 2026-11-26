// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/phase"
)

func TestStallCheck(t *testing.T) {
	t.Parallel()

	stale := func(p phase.Phase) *phase.Tracker {
		tr := phase.NewTracker()
		tr.Set(p)
		return tr
	}

	t.Run("a stream with no progress fails", func(t *testing.T) {
		t.Parallel()
		// an explicit timestamp, because a tracker built here would carry the
		// wall clock reading of this instant and leave the case resting on
		// clock resolution
		tr := &fakeTracker{phase: phase.Replication, last: time.Now().Add(-time.Hour)}
		err := stallCheck(tr, time.Minute)()
		require.Error(t, err)
		require.Contains(t, err.Error(), "no replication progress")
	})

	t.Run("a stream that just moved passes", func(t *testing.T) {
		t.Parallel()
		tr := stale(phase.Replication)
		tr.MarkProgress()
		require.NoError(t, stallCheck(tr, time.Hour)())
	})

	t.Run("a snapshot is exempt", func(t *testing.T) {
		t.Parallel()
		tr := stale(phase.Snapshot)
		require.NoError(t, stallCheck(tr, time.Nanosecond)())
	})

	t.Run("nothing reported yet is exempt", func(t *testing.T) {
		t.Parallel()
		tr := &fakeTracker{phase: phase.Replication}
		require.NoError(t, stallCheck(tr, time.Nanosecond)())
	})
}

// fakeTracker builds a state phase.Tracker cannot reach on its own: a phase
// that is set while nothing has reported progress in it.
type fakeTracker struct {
	phase phase.Phase
	last  time.Time
}

func (f *fakeTracker) Get() phase.Phase        { return f.phase }
func (f *fakeTracker) LastProgress() time.Time { return f.last }
