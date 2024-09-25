// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/schemalog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Store struct {
	inner  schemalog.Store
	tracer trace.Tracer
}

func NewStore(inner schemalog.Store, instrumentation *otel.Instrumentation) schemalog.Store {
	if instrumentation == nil {
		return inner
	}

	return &Store{
		inner:  inner,
		tracer: instrumentation.Tracer,
	}
}

func (s *Store) Insert(ctx context.Context, schemaName string) (le *schemalog.LogEntry, err error) {
	ctx, span := otel.StartSpan(ctx, s.tracer, "schemalogstore.Insert", trace.WithAttributes(attribute.String("schema", schemaName)))
	defer otel.CloseSpan(span, err)
	return s.inner.Insert(ctx, schemaName)
}

func (s *Store) Fetch(ctx context.Context, schemaName string, acked bool) (le *schemalog.LogEntry, err error) {
	ctx, span := otel.StartSpan(ctx, s.tracer, "schemalogstore.Fetch", trace.WithAttributes(attribute.String("schema", schemaName)))
	defer otel.CloseSpan(span, err)
	return s.inner.Fetch(ctx, schemaName, acked)
}

func (s *Store) Ack(ctx context.Context, le *schemalog.LogEntry) (err error) {
	ctx, span := otel.StartSpan(ctx, s.tracer, "schemalogstore.Ack", trace.WithAttributes(attribute.String("schema", le.SchemaName)))
	defer otel.CloseSpan(span, err)
	return s.inner.Ack(ctx, le)
}

func (s *Store) Close() error {
	return s.inner.Close()
}
