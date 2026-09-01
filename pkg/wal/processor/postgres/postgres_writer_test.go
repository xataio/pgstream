// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestNewWriter_droppedMetricsCarryTheWriterType pins the label a run with
// several writers is told apart by. Silently dropped rows are only visible
// through these counters, so a batch writer reporting under the bulk ingest
// writer's name — or under none — sends the loss to the wrong place.
//
// The pools are lazy, so no server is contacted: the writers are constructed,
// their metrics collected, and nothing is written.
func TestNewWriter_droppedMetricsCarryTheWriterType(t *testing.T) {
	t.Parallel()

	// distinct names are the whole point of the label
	require.NotEqual(t, batchWriter, bulkIngestWriter)

	tests := []struct {
		name string
		//nolint:revive // the constructors differ in return type, so a closure is the common shape
		newWriter func(context.Context, *Config, ...WriterOption) (interface{ Close() error }, error)

		wantWriterType string
	}{
		{
			name: "batch writer",
			newWriter: func(ctx context.Context, cfg *Config, opts ...WriterOption) (interface{ Close() error }, error) {
				return NewBatchWriter(ctx, cfg, opts...)
			},

			wantWriterType: "postgres_batch_writer",
		},
		{
			name: "bulk ingest writer",
			newWriter: func(ctx context.Context, cfg *Config, opts ...WriterOption) (interface{ Close() error }, error) {
				return NewBulkIngestWriter(ctx, cfg, opts...)
			},

			wantWriterType: "postgres_bulk_ingest_writer",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			instrumentation := &otel.Instrumentation{Meter: provider.Meter("test")}

			writer, err := tc.newWriter(context.Background(), &Config{
				URL:         "postgres://user:pass@localhost:5432/testdb",
				RetryPolicy: backoff.Config{DisableRetries: true},
			}, WithInstrumentation(instrumentation), WithLogger(loglib.NewNoopLogger()))
			require.NoError(t, err)
			defer writer.Close()

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))

			attrs := map[string]string{}
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					sum, ok := m.Data.(metricdata.Sum[int64])
					if !ok || len(sum.DataPoints) != 1 {
						continue
					}
					writerType, found := sum.DataPoints[0].Attributes.Value("writer_type")
					if !found {
						continue
					}
					attrs[m.Name] = writerType.AsString()
				}
			}

			require.Equal(t, tc.wantWriterType, attrs["pgstream.batch.sender.dropped_batches"])
			require.Equal(t, tc.wantWriterType, attrs["pgstream.batch.sender.dropped_messages"])
		})
	}
}
