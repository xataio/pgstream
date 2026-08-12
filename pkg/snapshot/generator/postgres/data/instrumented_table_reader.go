// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// instrumentedTableReader traces the reads of the reader it wraps. Since the
// reader hands out the sessions that do the reading, it also wraps every
// session it produces, so that the table reads are traced as children of the
// schema they belong to.
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

func (i *instrumentedTableReader) beginSchema(ctx context.Context, st *schemaTables, fn func(context.Context, readSession) error) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tableReader.BeginSchema", trace.WithAttributes([]attribute.KeyValue{
		{Key: "schema", Value: attribute.StringValue(st.schema)},
	}...))
	defer otel.CloseSpan(span, err)

	return i.reader.beginSchema(ctx, st, func(ctx context.Context, session readSession) error {
		return fn(ctx, &instrumentedReadSession{tracer: i.tracer, session: session})
	})
}

type instrumentedReadSession struct {
	tracer  trace.Tracer
	session readSession
}

func (i *instrumentedReadSession) readTable(ctx context.Context, table *table) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tableReader.ReadTable", trace.WithAttributes([]attribute.KeyValue{
		{Key: "schema", Value: attribute.StringValue(table.schema)},
		{Key: "table", Value: attribute.StringValue(table.name)},
	}...))
	defer otel.CloseSpan(span, err)
	return i.session.readTable(ctx, table)
}

func (i *instrumentedReadSession) totalBytes(ctx context.Context) (bytes int64, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tableReader.TotalBytes")
	defer otel.CloseSpan(span, err)
	return i.session.totalBytes(ctx)
}

func (i *instrumentedReadSession) logFields() loglib.Fields {
	return i.session.logFields()
}
