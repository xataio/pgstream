// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/phase"
	"github.com/xataio/pgstream/pkg/otel"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRegisterPhaseMetric_NoopCases(t *testing.T) {
	t.Parallel()

	tracker := phase.NewTracker()
	tracker.Set(phase.Snapshot)

	require.NoError(t, registerPhaseMetric(nil, tracker))
	require.NoError(t, registerPhaseMetric(&otel.Instrumentation{}, tracker))
	require.NoError(t, registerPhaseMetric(&otel.Instrumentation{Meter: nil}, tracker))
	_, instrumentation := testInstrumentation(t)
	require.NoError(t, registerPhaseMetric(instrumentation, nil))
}

func TestRegisterPhaseMetric_ObservesCurrentPhase(t *testing.T) {
	t.Parallel()

	reader, instrumentation := testInstrumentation(t)
	tracker := phase.NewTracker()

	require.NoError(t, registerPhaseMetric(instrumentation, tracker))

	tracker.Set(phase.Snapshot)
	points := collectPipelinePhaseMetric(t, reader)
	require.Len(t, points, 2)
	require.EqualValues(t, 1, valueForPhase(t, points, "snapshot"))
	require.EqualValues(t, 0, valueForPhase(t, points, "replication"))

	tracker.Set(phase.Replication)
	points = collectPipelinePhaseMetric(t, reader)
	require.Len(t, points, 2)
	require.EqualValues(t, 0, valueForPhase(t, points, "snapshot"))
	require.EqualValues(t, 1, valueForPhase(t, points, "replication"))
}

func valueForPhase(t *testing.T, points []metricdata.DataPoint[int64], phase string) int64 {
	t.Helper()

	for _, p := range points {
		if p.Attributes == attribute.NewSet(attribute.String("phase", phase)) {
			return p.Value
		}
	}
	t.Fatalf("no data point found for phase %q", phase)
	return 0
}

func TestRegisterPhaseMetric_EmptyPhaseNotObserved(t *testing.T) {
	t.Parallel()

	reader, instrumentation := testInstrumentation(t)
	tracker := phase.NewTracker()

	require.NoError(t, registerPhaseMetric(instrumentation, tracker))

	points := collectPipelinePhaseMetric(t, reader)
	require.Empty(t, points)
}

func testInstrumentation(t *testing.T) (*metric.ManualReader, *otel.Instrumentation) {
	t.Helper()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	return reader, &otel.Instrumentation{
		Meter: provider.Meter("test"),
	}
}

func collectPipelinePhaseMetric(t *testing.T, reader *metric.ManualReader) []metricdata.DataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "pgstream.pipeline.phase" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			return gauge.DataPoints
		}
	}
	return nil
}
