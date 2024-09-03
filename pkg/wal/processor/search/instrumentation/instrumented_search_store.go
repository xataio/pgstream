// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/processor/search"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type SearchStore struct {
	inner   search.Store
	tracer  trace.Tracer
	meter   metric.Meter
	metrics *storeMetrics
}

type storeMetrics struct {
	docErrors metric.Int64Counter
}

func NewStore(inner search.Store, instrumentation *otel.Instrumentation) (search.Store, error) {
	if instrumentation == nil {
		return inner, nil
	}

	s := &SearchStore{
		inner:   inner,
		tracer:  instrumentation.Tracer,
		meter:   instrumentation.Meter,
		metrics: &storeMetrics{},
	}

	if err := s.initMetrics(); err != nil {
		return nil, fmt.Errorf("error initialising search store metrics: %w", err)
	}

	return s, nil
}

func (s *SearchStore) ApplySchemaChange(ctx context.Context, logEntry *schemalog.LogEntry) (err error) {
	ctx, span := otel.StartSpan(ctx, s.tracer, "searchstore.ApplySchemaChange", trace.WithAttributes(
		attribute.String("schemaName", logEntry.SchemaName),
	))
	defer otel.CloseSpan(span, err)

	return s.inner.ApplySchemaChange(ctx, logEntry)
}

func (s *SearchStore) DeleteSchema(ctx context.Context, schemaName string) (err error) {
	ctx, span := otel.StartSpan(ctx, s.tracer, "searchstore.DeleteSchema", trace.WithAttributes(
		attribute.String("schemaName", schemaName),
	))
	defer otel.CloseSpan(span, err)

	return s.inner.DeleteSchema(ctx, schemaName)
}

func (s *SearchStore) DeleteTableDocuments(ctx context.Context, schemaName string, tableIDs []string) (err error) {
	ctx, span := otel.StartSpan(ctx, s.tracer, "searchstore.DeleteTableDocuments", trace.WithAttributes(
		attribute.String("schemaName", schemaName),
		attribute.StringSlice("tables", tableIDs),
	))
	defer otel.CloseSpan(span, err)

	return s.inner.DeleteTableDocuments(ctx, schemaName, tableIDs)
}

func (s *SearchStore) SendDocuments(ctx context.Context, docs []search.Document) (docErrs []search.DocumentError, err error) {
	ctx, span := otel.StartSpan(ctx, s.tracer, "searchstore.SendDocuments", trace.WithAttributes(
		attribute.Int("docCount", len(docs)),
	))
	defer otel.CloseSpan(span, err)

	docErrs, err = s.inner.SendDocuments(ctx, docs)
	if s.meter != nil && docErrs != nil {
		go func() {
			for _, docErr := range docErrs {
				s.metrics.docErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("severity", docErr.Severity.String())))
			}
		}()
	}

	return docErrs, err
}

func (s *SearchStore) GetMapper() search.Mapper {
	return s.inner.GetMapper()
}

func (s *SearchStore) initMetrics() error {
	if s.meter == nil {
		return nil
	}

	var err error
	s.metrics.docErrors, err = s.meter.Int64Counter("pgstream.search.store.doc.errors",
		metric.WithUnit("errors"),
		metric.WithDescription("Count of failed sent documents by severity"))
	if err != nil {
		return err
	}

	return nil
}
