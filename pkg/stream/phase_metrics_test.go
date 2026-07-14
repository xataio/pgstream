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
	require.Len(t, points, 1)
	require.EqualValues(t, 1, points[0].Value)
	require.Equal(t, attribute.NewSet(attribute.String("phase", "snapshot")), points[0].Attributes)

	tracker.Set(phase.Replication)
	points = collectPipelinePhaseMetric(t, reader)
	require.Len(t, points, 1)
	require.EqualValues(t, 1, points[0].Value)
	require.Equal(t, attribute.NewSet(attribute.String("phase", "replication")), points[0].Attributes)
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
