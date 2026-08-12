// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/cmd/config"
	"github.com/xataio/pgstream/internal/health"
	"github.com/xataio/pgstream/internal/phase"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
)

const (
	// healthShutdownTimeout caps how long graceful shutdown of the health
	// server and its readiness pool can take before we move on.
	healthShutdownTimeout = 5 * time.Second
	// healthReadinessPoolMaxConns sizes the postgres pool used by /ready.
	// Two connections cover concurrent probe traffic with negligible cost.
	healthReadinessPoolMaxConns = 2
)

// startHealthServer starts the optional health endpoint, returning a closer
// that gracefully shuts it down. When health is disabled the closer is a
// noop. Bind errors are surfaced synchronously so a misconfigured port
// fails the command immediately rather than silently degrading.
//
// sourcePostgresURL, when non-empty, wires the /ready endpoint to ping the
// source database. phaseTracker, when non-nil, wires the /status endpoint to
// report the current pipeline phase.
func startHealthServer(ctx context.Context, logger loglib.Logger, sourcePostgresURL string, phaseTracker *phase.Tracker) (func(), error) {
	cfg, err := config.ParseHealthConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing health config: %w", err)
	}
	if !cfg.Enabled {
		return func() {}, nil
	}

	instrumentationCfg, err := config.ParseInstrumentationConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing instrumentation config: %w", err)
	}

	opts := []health.Option{
		health.WithLogger(logger),
		health.WithVersion(version()),
	}

	if phaseTracker != nil {
		opts = append(opts, health.WithPhaseProvider(func() string {
			return string(phaseTracker.Get())
		}))
	}

	var pool *pglib.Pool
	if sourcePostgresURL != "" {
		pool, err = pglib.NewConnPool(ctx, sourcePostgresURL, pglib.WithMaxConnections(healthReadinessPoolMaxConns))
		if err != nil {
			return nil, fmt.Errorf("creating health readiness pool: %w", err)
		}
		opts = append(opts, health.WithReadinessCheck(pool.Ping))
	}

	var metricsCfg *otel.MetricsConfig
	if instrumentationCfg.Metrics != nil {
		metricsCfg = instrumentationCfg.Metrics
	}
	srv := health.NewServer(*cfg, metricsCfg, opts...)
	if err := srv.Listen(); err != nil {
		if pool != nil {
			pool.Close(context.Background())
		}
		return nil, fmt.Errorf("binding health server: %w", err)
	}
	go func() {
		if err := srv.Serve(); err != nil {
			logger.Error(err, "health server stopped")
		}
	}()

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), healthShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Warn(err, "shutting down health server")
		}
		if pool != nil {
			if err := pool.Close(shutdownCtx); err != nil {
				logger.Warn(err, "closing health readiness pool")
			}
		}
	}, nil
}
