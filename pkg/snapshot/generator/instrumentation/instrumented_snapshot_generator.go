// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type SnapshotGenerator struct {
	inner   generator.SnapshotGenerator
	tracer  trace.Tracer
	meter   metric.Meter
	metrics *metrics
}

type metrics struct {
	snapshotLatency metric.Int64Histogram
}

const (
	snapshotSchemaAttributeKey = "snapshot_schema"
	snapshotTablesAttributeKey = "snapshot_tables"
)

func NewSnapshotGenerator(g generator.SnapshotGenerator, instrumentation *otel.Instrumentation) (generator.SnapshotGenerator, error) {
	if instrumentation == nil {
		return g, nil
	}

	generator := &SnapshotGenerator{
		inner:   g,
		tracer:  instrumentation.Tracer,
		meter:   instrumentation.Meter,
		metrics: &metrics{},
	}

	if err := generator.initMetrics(); err != nil {
		return nil, fmt.Errorf("initialising snapshot generator metrics: %w", err)
	}

	return generator, nil
}

func (i *SnapshotGenerator) CreateSnapshot(ctx context.Context, snapshot *snapshot.Snapshot) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "snapshotGenerator.CreateSnapshot", trace.WithAttributes(i.snapshotAttributes(snapshot)...))
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.snapshotLatency.Record(ctx, int64(time.Since(startTime).Milliseconds()), metric.WithAttributes(i.snapshotAttributes(snapshot)...))
		}()
	}
	return i.inner.CreateSnapshot(ctx, snapshot)
}

func (i *SnapshotGenerator) Close() error {
	return i.inner.Close()
}

func (i *SnapshotGenerator) initMetrics() error {
	if i.meter == nil {
		return nil
	}

	var err error
	i.metrics.snapshotLatency, err = i.meter.Int64Histogram("pgstream.snapshot.generator.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of the time taken to snapshot a source postgres database"))
	if err != nil {
		return err
	}

	return nil
}

func (i *SnapshotGenerator) snapshotAttributes(s *snapshot.Snapshot) []attribute.KeyValue {
	return []attribute.KeyValue{
		{
			Key:   snapshotSchemaAttributeKey,
			Value: attribute.StringSliceValue(s.GetSchemas()),
		},
		{
			Key:   snapshotTablesAttributeKey,
			Value: attribute.StringSliceValue(s.GetTables()),
		},
	}
}
