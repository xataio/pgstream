// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	snapshotlistener "github.com/xataio/pgstream/pkg/wal/listener/snapshot"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"golang.org/x/sync/errgroup"
)

// Snapshot performs a one-time data snapshot. This call is blocking.
// Pass WithPhaseTracker to expose the snapshot phase via /status and metrics.
func Snapshot(ctx context.Context, logger loglib.Logger, config *Config, instrumentation *otel.Instrumentation, opts ...InitOption) error {
	if config.Listener.Postgres == nil {
		return errors.New("source postgres snapshot not configured: ensure source.postgres is set")
	}

	if err := config.IsValid(); err != nil {
		return fmt.Errorf("incompatible configuration: %w", err)
	}

	tracker := config.GetInitConfig(opts...).PhaseTracker

	if err := registerPhaseMetric(instrumentation, tracker); err != nil {
		return fmt.Errorf("registering pipeline phase metric: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)

	// Processor

	chain, closer, err := newProcessor(ctx, logger, config, nil, processorTypeSnapshot, instrumentation)
	defer closer()
	if err != nil {
		return err
	}

	// Listener

	config.applySnapshotRawJSONValues()
	config.applySnapshotCopyPassthrough(chain)
	snapshotGenerator, err := snapshotbuilder.NewSnapshotGenerator(
		ctx,
		config.Listener.Postgres.Snapshot,
		chain.processor,
		logger,
		instrumentation,
		config.restoreConflictTargetsBeforeData())
	if err != nil {
		return err
	}
	listener := snapshotlistener.New(snapshotGenerator, snapshotlistener.WithPhaseTracker(tracker))
	defer listener.Close()

	eg.Go(func() error {
		defer logger.Info("stopping postgres snapshot listener...")
		logger.Info("running postgres snapshot listener...")
		return listener.Listen(ctx)
	})

	if err := eg.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	return nil
}
