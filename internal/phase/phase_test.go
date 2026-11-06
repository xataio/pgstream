// SPDX-License-Identifier: Apache-2.0

package phase

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTracker_GetSet(t *testing.T) {
	t.Parallel()

	tr := NewTracker()
	require.Equal(t, Phase(""), tr.Get())

	tr.Set(Snapshot)
	require.Equal(t, Snapshot, tr.Get())

	tr.Set(Replication)
	require.Equal(t, Replication, tr.Get())
}

func TestTracker_NilSafe(t *testing.T) {
	t.Parallel()

	var tr *Tracker
	require.Equal(t, Phase(""), tr.Get())
	tr.Set(Snapshot) // must not panic
	require.Equal(t, Phase(""), tr.Get())
}

func TestTracker_Progress(t *testing.T) {
	t.Parallel()

	tr := NewTracker()
	require.True(t, tr.LastProgress().IsZero())

	before := time.Now()
	tr.MarkProgress()
	require.False(t, tr.LastProgress().Before(before))

	// entering a phase counts as progress
	tr2 := NewTracker()
	tr2.Set(Replication)
	require.False(t, tr2.LastProgress().IsZero())
}

func TestTracker_ProgressNilSafe(t *testing.T) {
	t.Parallel()

	var tr *Tracker
	tr.MarkProgress() // must not panic
	require.True(t, tr.LastProgress().IsZero())
}
