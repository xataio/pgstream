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

func Snapshot(ctx context.Context, logger loglib.Logger, config *Config, instrumentation *otel.Instrumentation) error {
	if config.Listener.Snapshot == nil {
		return errors.New("source snapshot not configured")
	}

	if err := config.IsValid(); err != nil {
		return fmt.Errorf("incompatible configuration: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)

	// Processor

	processor, err := buildProcessor(ctx, logger, &config.Processor, nil, instrumentation)
	if err != nil {
		return err
	}
	defer processor.Close()

	var closer closerFn
	processor, closer, err = addProcessorModifiers(ctx, config, logger, processor, instrumentation)
	if err != nil {
		return err
	}
	defer closer()

	// Listener

	snapshotGenerator, err := snapshotbuilder.NewSnapshotGenerator(
		ctx,
		config.Listener.Snapshot,
		processor.ProcessWALEvent,
		logger)
	if err != nil {
		return err
	}
	listener := snapshotlistener.New(snapshotGenerator)
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
