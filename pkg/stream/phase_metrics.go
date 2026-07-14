// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/internal/phase"
	"github.com/xataio/pgstream/pkg/otel"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// registerPhaseMetric registers an observable gauge that reports the current
// pipeline phase. No-op when instrumentation or the tracker is nil.
func registerPhaseMetric(instrumentation *otel.Instrumentation, tracker *phase.Tracker) error {
	if instrumentation == nil || !instrumentation.IsEnabled() || tracker == nil {
		return nil
	}
	if instrumentation.Meter == nil {
		return nil
	}

	gauge, err := instrumentation.Meter.Int64ObservableGauge("pgstream.pipeline.phase",
		metric.WithUnit("1"),
		metric.WithDescription("Current pipeline phase: reports 1 with attribute phase=snapshot|replication"))
	if err != nil {
		return fmt.Errorf("creating pipeline phase gauge: %w", err)
	}

	_, err = instrumentation.Meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		p := tracker.Get()
		if p == "" {
			return nil
		}
		o.ObserveInt64(gauge, 1, metric.WithAttributes(attribute.String("phase", string(p))))
		return nil
	}, gauge)
	if err != nil {
		return fmt.Errorf("registering pipeline phase callback: %w", err)
	}
	return nil
}
