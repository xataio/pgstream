// SPDX-License-Identifier: Apache-2.0

// Package phase tracks whether a running pgstream process is in the initial
// snapshot or logical replication (streaming) phase.
package phase

import (
	"sync/atomic"
	"time"
)

// Phase identifies the current pipeline stage.
type Phase string

const (
	// Snapshot is the initial data/schema snapshot stage.
	Snapshot Phase = "snapshot"
	// Replication is the logical replication / streaming stage.
	Replication Phase = "replication"
)

// Tracker holds the current pipeline phase and the time the pipeline last
// reported progress. It is safe for concurrent use.
type Tracker struct {
	current atomic.Value // stores Phase
	// lastProgress holds unix nanoseconds, and zero while nothing has been
	// reported. A liveness check reads it to tell a stream that is quiet from
	// one that is gone.
	lastProgress atomic.Int64
}

// NewTracker returns a tracker with an empty phase until the first Set.
func NewTracker() *Tracker {
	t := &Tracker{}
	t.current.Store(Phase(""))
	return t
}

// Set updates the current phase. Entering a phase is progress.
func (t *Tracker) Set(p Phase) {
	if t == nil {
		return
	}
	t.current.Store(p)
	t.MarkProgress()
}

// MarkProgress records that the pipeline has just moved.
func (t *Tracker) MarkProgress() {
	if t == nil {
		return
	}
	t.lastProgress.Store(time.Now().UnixNano())
}

// LastProgress returns the time of the last MarkProgress, or the zero time
// when there has been none.
func (t *Tracker) LastProgress() time.Time {
	if t == nil {
		return time.Time{}
	}
	ns := t.lastProgress.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// Get returns the current phase, or empty if unset / tracker is nil.
func (t *Tracker) Get() Phase {
	if t == nil {
		return ""
	}
	v := t.current.Load()
	p, ok := v.(Phase)
	if !ok {
		return ""
	}
	return p
}
