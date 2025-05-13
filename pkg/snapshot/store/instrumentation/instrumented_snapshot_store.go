// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/store"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Store struct {
	inner  store.Store
	tracer trace.Tracer
}

func NewStore(s store.Store, instrumentation *otel.Instrumentation) store.Store {
	if instrumentation == nil {
		return s
	}

	return &Store{
		inner:  s,
		tracer: instrumentation.Tracer,
	}
}

func (i *Store) CreateSnapshotRequest(ctx context.Context, req *snapshot.Request) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "snapshotStore.CreateSnapshotRequest", trace.WithAttributes(i.requestAttributes(req)...))
	defer otel.CloseSpan(span, err)

	return i.inner.CreateSnapshotRequest(ctx, req)
}

func (i *Store) UpdateSnapshotRequest(ctx context.Context, req *snapshot.Request) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "snapshotStore.UpdateSnapshotRequest", trace.WithAttributes(i.requestAttributes(req)...))
	defer otel.CloseSpan(span, err)

	return i.inner.UpdateSnapshotRequest(ctx, req)
}

func (i *Store) GetSnapshotRequestsByStatus(ctx context.Context, status snapshot.Status) (req []*snapshot.Request, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "snapshotStore.GetSnapshotRequestsByStatus",
		trace.WithAttributes(attribute.KeyValue{Key: "status", Value: attribute.StringValue(string(status))}))
	defer otel.CloseSpan(span, err)

	return i.inner.GetSnapshotRequestsByStatus(ctx, status)
}

func (i *Store) GetSnapshotRequestsBySchema(ctx context.Context, schema string) (req []*snapshot.Request, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "snapshotStore.GetSnapshotRequestsBySchema",
		trace.WithAttributes(attribute.KeyValue{Key: "schema", Value: attribute.StringValue(schema)}))
	defer otel.CloseSpan(span, err)

	return i.inner.GetSnapshotRequestsBySchema(ctx, schema)
}

func (i *Store) Close() error {
	return i.inner.Close()
}

func (i *Store) requestAttributes(req *snapshot.Request) []attribute.KeyValue {
	return []attribute.KeyValue{
		{Key: "schema", Value: attribute.StringValue(req.Snapshot.SchemaName)},
		{Key: "tables", Value: attribute.StringSliceValue(req.Snapshot.TableNames)},
		{Key: "status", Value: attribute.StringValue(string(req.Status))},
	}
}
