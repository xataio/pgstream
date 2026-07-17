// SPDX-License-Identifier: Apache-2.0

package phase

import (
	"testing"

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
