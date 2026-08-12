// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/stream"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// the exported names are a contract with whatever scrapes them, so the test
// spells them out rather than reaching for the constants in the batch package
const (
	droppedBatchesMetric  = "pgstream.batch.sender.dropped_batches"
	droppedMessagesMetric = "pgstream.batch.sender.dropped_messages"
)

// Test_PostgresToPostgres_DroppedBatchMetrics observes what a running pipeline
// exports when ignore_send_errors discards a batch. The target is seeded with a
// row the replicated insert collides with: strict mode turns the duplicate key
// into a batch send failure, and ignore_send_errors drops the batch rather than
// stopping the pipeline. That silent loss is only visible through these
// counters, so the test asserts on them while the stream is still running.
func Test_PostgresToPostgres_DroppedBatchMetrics(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	reader, instrumentation := testMetricsInstrumentation()

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testPostgresProcessorCfg(withStrictMode(), withIgnoreSendErrors()),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStreamWithInstrumentation(t, ctx, cfg, instrumentation)

	testTable := "pg2pg_dropped_batch_metrics_test"

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	execQuery(t, ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, name text)", testTable))
	defer execQuery(t, ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))

	require.Eventually(t, func() bool {
		return len(getInformationSchemaColumns(t, ctx, targetConn, testTable)) == 2
	}, 20*time.Second, 200*time.Millisecond, "table schema not replicated")

	// nothing has failed to send yet, so the counters are exported at zero
	// rather than being absent
	dropped := collectDroppedBatchMetrics(t, ctx, reader)
	require.Equal(t, int64(0), dropped[droppedBatchesMetric].value)
	require.Equal(t, int64(0), dropped[droppedMessagesMetric].value)

	// seed the row the replicated insert cannot be written over
	execQueryWithURL(t, ctx, targetPGURL, fmt.Sprintf(
		"INSERT INTO %s(id, name) VALUES (1, 'stale-target-row')", testTable))

	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(id, name) VALUES (1, 'source-row')", testTable))

	require.Eventually(t, func() bool {
		dropped = collectDroppedBatchMetrics(t, ctx, reader)
		return dropped[droppedBatchesMetric].value > 0
	}, 30*time.Second, 200*time.Millisecond, "dropped batch counters were never incremented")

	// the batch size is 1, so the failed insert is a batch of exactly one
	// message, and both counters have to agree on that
	require.Equal(t, int64(1), dropped[droppedBatchesMetric].value)
	require.Equal(t, int64(1), dropped[droppedMessagesMetric].value)

	// the label has to name the writer that dropped, so a run with several
	// writers can be told apart
	require.Equal(t, batchWriterType, dropped[droppedBatchesMetric].writerType)
	require.Equal(t, batchWriterType, dropped[droppedMessagesMetric].writerType)

	// the source row is gone: it was never written, which is the loss the
	// counters are reporting
	var count int
	require.NoError(t, targetConn.QueryRow(ctx, []any{&count},
		fmt.Sprintf("SELECT count(*) FROM %s WHERE name = 'source-row'", testTable)))
	require.Zero(t, count, "the dropped row must not have reached the target")

	// dropping the batch left the pipeline running: the next insert replicates
	execQuery(t, ctx, fmt.Sprintf("INSERT INTO %s(id, name) VALUES (2, 'after-drop')", testTable))
	require.Eventually(t, func() bool {
		var name string
		if err := targetConn.QueryRow(ctx, []any{&name},
			fmt.Sprintf("SELECT name FROM %s WHERE id = 2", testTable)); err != nil {
			return false
		}
		return name == "after-drop"
	}, 30*time.Second, 200*time.Millisecond, "pipeline stopped replicating after the dropped batch")

	// and a successful send does not move the counters
	dropped = collectDroppedBatchMetrics(t, ctx, reader)
	require.Equal(t, int64(1), dropped[droppedBatchesMetric].value)
	require.Equal(t, int64(1), dropped[droppedMessagesMetric].value)
}

const batchWriterType = "postgres_batch_writer"

// testMetricsInstrumentation returns instrumentation backed by a manual reader,
// so a test can collect the exported metrics on demand.
func testMetricsInstrumentation() (*sdkmetric.ManualReader, *otel.Instrumentation) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return reader, &otel.Instrumentation{Meter: provider.Meter("integration_test")}
}

type droppedMetric struct {
	value      int64
	writerType string
}

// collectDroppedBatchMetrics reads the drop counters exported by the batch
// senders. Collect runs their observable callbacks, so the values are the ones
// the pipeline holds at this instant.
func collectDroppedBatchMetrics(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader) map[string]droppedMetric {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	metrics := map[string]droppedMetric{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != droppedBatchesMetric && m.Name != droppedMessagesMetric {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %s is not an int64 sum", m.Name)
			require.Len(t, sum.DataPoints, 1, "metric %s has more than one writer reporting", m.Name)

			writerType, found := sum.DataPoints[0].Attributes.Value("writer_type")
			require.True(t, found, "metric %s is missing the writer_type attribute", m.Name)
			metrics[m.Name] = droppedMetric{
				value:      sum.DataPoints[0].Value,
				writerType: writerType.AsString(),
			}
		}
	}

	require.Contains(t, metrics, droppedBatchesMetric, "%s was not exported", droppedBatchesMetric)
	require.Contains(t, metrics, droppedMessagesMetric, "%s was not exported", droppedMessagesMetric)
	return metrics
}
