// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/trace"
)

type Tx struct {
	inner  pglib.Tx
	tracer trace.Tracer
}

func NewTx(t pglib.Tx, instrumentation *otel.Instrumentation) pglib.Tx {
	if instrumentation == nil {
		return t
	}

	return &Tx{
		inner:  t,
		tracer: instrumentation.Tracer,
	}
}

func (i *Tx) Query(ctx context.Context, query string, args ...any) (rows pglib.Rows, err error) {
	queryAttrs := queryAttributes(query)
	ctx, span := otel.StartSpan(ctx, i.tracer, "tx.Query", trace.WithAttributes(queryAttrs...))
	defer otel.CloseSpan(span, err)
	return i.inner.Query(ctx, query, args...)
}

func (i *Tx) QueryRow(ctx context.Context, dest []any, query string, args ...any) error {
	queryAttrs := queryAttributes(query)
	ctx, span := otel.StartSpan(ctx, i.tracer, "tx.QueryRow", trace.WithAttributes(queryAttrs...))
	defer otel.CloseSpan(span, nil)
	return i.inner.QueryRow(ctx, dest, query, args...)
}

func (i *Tx) Exec(ctx context.Context, query string, args ...any) (tag pglib.CommandTag, err error) {
	queryAttrs := queryAttributes(query)
	ctx, span := otel.StartSpan(ctx, i.tracer, "tx.Exec", trace.WithAttributes(queryAttrs...))
	defer otel.CloseSpan(span, err)
	return i.inner.Exec(ctx, query, args...)
}

func (i *Tx) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (rowCount int64, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "tx.CopyFrom")
	defer otel.CloseSpan(span, err)
	return i.inner.CopyFrom(ctx, tableName, columnNames, srcRows)
}
