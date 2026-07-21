// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"

	"github.com/xataio/pgstream/internal/phase"
)

type Generator interface {
	CreateSnapshot(context.Context) error
	Close() error
}

type Listener struct {
	generator    Generator
	phaseTracker *phase.Tracker
}

type Option func(*Listener)

func New(generator Generator, opts ...Option) *Listener {
	l := &Listener{
		generator: generator,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// WithPhaseTracker registers a tracker set to snapshot when Listen starts.
func WithPhaseTracker(t *phase.Tracker) Option {
	return func(l *Listener) {
		l.phaseTracker = t
	}
}

// Listen starts the snapshot generation process.
func (l *Listener) Listen(ctx context.Context) error {
	l.phaseTracker.Set(phase.Snapshot)
	return l.generator.CreateSnapshot(ctx)
}

// Close closes the listener internal resources
func (l *Listener) Close() error {
	return l.generator.Close()
}
