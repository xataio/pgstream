// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"errors"

	"github.com/xataio/pgstream/pkg/snapshot"
)

// GeneratorAggregator aggregates snapshot generators
type GeneratorAggregator struct {
	generators []SnapshotGenerator
}

// NewAggregator will aggregate the generators on input.
func NewAggregator(generators []SnapshotGenerator) *GeneratorAggregator {
	return &GeneratorAggregator{
		generators: generators,
	}
}

// CreateSnapshot will call the CreateSnapshot method for each of the aggregated
// snapshot generators sequentially, in the order in which they were provided.
// If one of them fails, an error will be returned without continuing with the
// rest of generator calls.
func (a *GeneratorAggregator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	for _, gen := range a.generators {
		if err := gen.CreateSnapshot(ctx, ss); err != nil {
			return err
		}
	}
	return nil
}

// Close will aggregate the errors of the generator close calls.
func (a *GeneratorAggregator) Close() error {
	var closeErrs error
	for _, gen := range a.generators {
		if err := gen.Close(); err != nil {
			if closeErrs != nil {
				closeErrs = errors.Join(closeErrs, err)
			} else {
				closeErrs = err
			}
		}
	}
	return closeErrs
}
