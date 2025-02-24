// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
)

type Generator interface {
	CreateSnapshot(context.Context) error
	Close() error
}

type Listener struct {
	generator Generator
}

func New(generator Generator) *Listener {
	return &Listener{
		generator: generator,
	}
}

// Listen starts the snapshot generation process.
func (l *Listener) Listen(ctx context.Context) error {
	return l.generator.CreateSnapshot(ctx)
}

// Close closes the listener internal resources
func (l *Listener) Close() error {
	return l.generator.Close()
}
