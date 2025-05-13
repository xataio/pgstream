// SPDX-License-Identifier: Apache-2.0

package tablefinder

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type tableDiscoverer struct {
	fn     tableDiscoveryFn
	tracer trace.Tracer
}

func newInstrumentedTableDiscoveryFn(fn tableDiscoveryFn, i *otel.Instrumentation) tableDiscoveryFn {
	td := tableDiscoverer{
		fn:     fn,
		tracer: i.Tracer,
	}
	return td.discoverTables
}

func (i *tableDiscoverer) discoverTables(ctx context.Context, conn pglib.Querier, schema string) (tables []string, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tableFinder.discoverTables", trace.WithAttributes(attribute.KeyValue{
		Key: "schema", Value: attribute.StringValue(schema),
	}))
	defer otel.CloseSpan(span, err)

	return i.fn(ctx, conn, schema)
}
