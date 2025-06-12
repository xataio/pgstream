// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type instrumentedTableSnapshotGenerator struct {
	tracer          trace.Tracer
	snapshotTableFn snapshotTableFn
}

func newInstrumentedTableSnapshotGenerator(fn snapshotTableFn, i *otel.Instrumentation) *instrumentedTableSnapshotGenerator {
	return &instrumentedTableSnapshotGenerator{
		tracer:          i.Tracer,
		snapshotTableFn: fn,
	}
}

func (i *instrumentedTableSnapshotGenerator) snapshotTable(ctx context.Context, snapshotID string, table *table) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tableSnapshotGenerator.SnapshotTable", trace.WithAttributes([]attribute.KeyValue{
		{Key: "schema", Value: attribute.StringValue(table.schema)},
		{Key: "table", Value: attribute.StringValue(table.name)},
	}...))
	defer otel.CloseSpan(span, err)
	return i.snapshotTableFn(ctx, snapshotID, table)
}
