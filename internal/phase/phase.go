// SPDX-License-Identifier: Apache-2.0

// Package phase tracks whether a running pgstream process is in the initial
// snapshot or logical replication (streaming) phase.
package phase

import "sync/atomic"

// Phase identifies the current pipeline stage.
type Phase string

const (
	// Snapshot is the initial data/schema snapshot stage.
	Snapshot Phase = "snapshot"
	// Replication is the logical replication / streaming stage.
	Replication Phase = "replication"
)

// Tracker holds the current pipeline phase. It is safe for concurrent use.
type Tracker struct {
	current atomic.Value // stores Phase
}

// NewTracker returns a tracker with an empty phase until the first Set.
func NewTracker() *Tracker {
	t := &Tracker{}
	t.current.Store(Phase(""))
	return t
}

// Set updates the current phase.
func (t *Tracker) Set(p Phase) {
	if t == nil {
		return
	}
	t.current.Store(p)
}

// Get returns the current phase, or empty if unset / tracker is nil.
func (t *Tracker) Get() Phase {
	if t == nil {
		return ""
	}
	v := t.current.Load()
	if v == nil {
		return ""
	}
	return v.(Phase)
}
