// SPDX-License-Identifier: Apache-2.0

package tablefinder

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type schemaTableDiscoverer struct {
	tableDiscoveryFn  tableDiscoveryFn
	schemaDiscoveryFn schemaDiscoveryFn
	tracer            trace.Tracer
}

func newInstrumentedSchemaTableDiscoveryFns(schemaFn schemaDiscoveryFn, tableFn tableDiscoveryFn, i *otel.Instrumentation) (schemaDiscoveryFn, tableDiscoveryFn) {
	td := schemaTableDiscoverer{
		schemaDiscoveryFn: schemaFn,
		tableDiscoveryFn:  tableFn,
		tracer:            i.Tracer,
	}
	return td.discoverSchemas, td.discoverTables
}

func (i *schemaTableDiscoverer) discoverSchemas(ctx context.Context, conn pglib.Querier) (tables []string, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tableFinder.discoverSchemas")
	defer otel.CloseSpan(span, err)
	return i.schemaDiscoveryFn(ctx, conn)
}

func (i *schemaTableDiscoverer) discoverTables(ctx context.Context, conn pglib.Querier, schema string) (tables []string, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tableFinder.discoverTables", trace.WithAttributes(attribute.KeyValue{
		Key: "schema", Value: attribute.StringValue(schema),
	}))
	defer otel.CloseSpan(span, err)

	return i.tableDiscoveryFn(ctx, conn, schema)
}
