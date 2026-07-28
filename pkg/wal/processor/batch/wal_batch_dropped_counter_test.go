// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestDroppedCounter_record(t *testing.T) {
	t.Parallel()

	c := NewDroppedCounter()
	require.Equal(t, uint64(0), c.Batches())
	require.Equal(t, uint64(0), c.Messages())

	batches, messages := c.record(10)
	require.Equal(t, uint64(1), batches)
	require.Equal(t, uint64(10), messages)

	batches, messages = c.record(5)
	require.Equal(t, uint64(2), batches)
	require.Equal(t, uint64(15), messages)

	require.Equal(t, uint64(2), c.Batches())
	require.Equal(t, uint64(15), c.Messages())
}

func TestDroppedCounter_record_concurrent(t *testing.T) {
	t.Parallel()

	// the drainer pool records concurrently, and the totals returned by record
	// are what the per-drop log line reports: every caller must see a distinct
	// batch total, or two log lines claim the same drop
	const senders, drops = 8, 50

	c := NewDroppedCounter()
	seen := make(chan uint64, senders*drops)

	wg := sync.WaitGroup{}
	for range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range drops {
				batches, _ := c.record(2)
				seen <- batches
			}
		}()
	}
	wg.Wait()
	close(seen)

	require.Equal(t, uint64(senders*drops), c.Batches())
	require.Equal(t, uint64(senders*drops*2), c.Messages())

	totals := map[uint64]struct{}{}
	for total := range seen {
		require.NotContains(t, totals, total, "two callers reported the same running total")
		totals[total] = struct{}{}
	}
	require.Len(t, totals, senders*drops)
}

func TestDroppedCounter_LogTotals(t *testing.T) {
	t.Parallel()

	t.Run("nothing dropped stays silent", func(t *testing.T) {
		t.Parallel()
		NewDroppedCounter().LogTotals(log.NewNoopLogger())
	})

	t.Run("drops are reported", func(t *testing.T) {
		t.Parallel()
		c := NewDroppedCounter()
		c.record(3)
		c.LogTotals(log.NewNoopLogger())
		require.Equal(t, uint64(1), c.Batches())
	})
}

func TestDroppedCounter_RegisterMetrics(t *testing.T) {
	t.Parallel()

	t.Run("no instrumentation is a no-op", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, NewDroppedCounter().RegisterMetrics(nil, "test_writer"))
		require.NoError(t, NewDroppedCounter().RegisterMetrics(&otel.Instrumentation{}, "test_writer"))
	})

	t.Run("counters are exported with the writer type", func(t *testing.T) {
		t.Parallel()

		reader := metric.NewManualReader()
		provider := metric.NewMeterProvider(metric.WithReader(reader))
		instrumentation := &otel.Instrumentation{Meter: provider.Meter("test")}

		c := NewDroppedCounter()
		require.NoError(t, c.RegisterMetrics(instrumentation, "postgres_bulk_ingest_writer"))

		c.record(25000)
		c.record(25000)

		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))

		got := map[string]int64{}
		attrs := map[string]string{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok, "metric %s is not an int64 sum", m.Name)
				require.Len(t, sum.DataPoints, 1)
				got[m.Name] = sum.DataPoints[0].Value
				writerType, found := sum.DataPoints[0].Attributes.Value("writer_type")
				require.True(t, found, "metric %s is missing the writer_type attribute", m.Name)
				attrs[m.Name] = writerType.AsString()
			}
		}

		require.Equal(t, int64(2), got[droppedBatchesMetricName])
		require.Equal(t, int64(50000), got[droppedMessagesMetricName])
		require.Equal(t, "postgres_bulk_ingest_writer", attrs[droppedBatchesMetricName])
		require.Equal(t, "postgres_bulk_ingest_writer", attrs[droppedMessagesMetricName])
	})
}
