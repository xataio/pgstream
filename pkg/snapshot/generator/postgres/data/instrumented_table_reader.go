// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type instrumentedTableReader struct {
	tracer trace.Tracer
	reader tableReader
}

func newInstrumentedTableReader(reader tableReader, i *otel.Instrumentation) *instrumentedTableReader {
	return &instrumentedTableReader{
		tracer: i.Tracer,
		reader: reader,
	}
}

func (i *instrumentedTableReader) beginSchema(ctx context.Context, st *schemaTables, fn func(context.Context, *readSession) error) error {
	return i.reader.beginSchema(ctx, st, fn)
}

func (i *instrumentedTableReader) readTable(ctx context.Context, session *readSession, table *table) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tableReader.ReadTable", trace.WithAttributes([]attribute.KeyValue{
		{Key: "schema", Value: attribute.StringValue(table.schema)},
		{Key: "table", Value: attribute.StringValue(table.name)},
	}...))
	defer otel.CloseSpan(span, err)
	return i.reader.readTable(ctx, session, table)
}
