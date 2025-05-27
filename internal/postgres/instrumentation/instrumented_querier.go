// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"
	"strings"
	"time"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/otel"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Querier struct {
	inner   pglib.Querier
	tracer  trace.Tracer
	meter   metric.Meter
	metrics *metrics
}

type metrics struct {
	queryLatency metric.Int64Histogram
}

const (
	queryTypeAttributeKey = "query_type"
	queryAttributeKey     = "query"
	unknownQueryType      = "unknown"
	txQueryType           = "tx"
)

func NewQuerier(q pglib.Querier, instrumentation *otel.Instrumentation) (pglib.Querier, error) {
	if instrumentation == nil {
		return q, nil
	}

	querier := &Querier{
		inner:   q,
		tracer:  instrumentation.Tracer,
		meter:   instrumentation.Meter,
		metrics: &metrics{},
	}

	if err := querier.initMetrics(); err != nil {
		return nil, fmt.Errorf("initialising postgres querier metrics: %w", err)
	}

	return querier, nil
}

func (i *Querier) Query(ctx context.Context, query string, args ...any) (rows pglib.Rows, err error) {
	queryAttrs := queryAttributes(query)
	ctx, span := otel.StartSpan(ctx, i.tracer, "querier.Query", trace.WithAttributes(queryAttrs...))
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.queryLatency.Record(ctx, int64(time.Since(startTime).Milliseconds()), metric.WithAttributes(queryAttrs...))
		}()
	}
	return i.inner.Query(ctx, query, args...)
}

func (i *Querier) QueryRow(ctx context.Context, query string, args ...any) pglib.Row {
	queryAttrs := queryAttributes(query)
	ctx, span := otel.StartSpan(ctx, i.tracer, "querier.QueryRow", trace.WithAttributes(queryAttrs...))
	defer otel.CloseSpan(span, nil)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.queryLatency.Record(ctx, int64(time.Since(startTime).Milliseconds()), metric.WithAttributes(queryAttrs...))
		}()
	}
	return i.inner.QueryRow(ctx, query, args...)
}

func (i *Querier) Exec(ctx context.Context, query string, args ...any) (tag pglib.CommandTag, err error) {
	queryAttrs := queryAttributes(query)
	ctx, span := otel.StartSpan(ctx, i.tracer, "querier.Exec", trace.WithAttributes(queryAttrs...))
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.queryLatency.Record(ctx, int64(time.Since(startTime).Milliseconds()), metric.WithAttributes(queryAttrs...))
		}()
	}
	return i.inner.Exec(ctx, query, args...)
}

func (i *Querier) ExecInTx(ctx context.Context, fn func(tx pglib.Tx) error) (err error) {
	queryAttrs := queryAttributes(txQueryType)
	ctx, span := otel.StartSpan(ctx, i.tracer, "querier.ExecInTx", trace.WithAttributes(queryAttrs...))
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.queryLatency.Record(ctx, int64(time.Since(startTime).Milliseconds()), metric.WithAttributes(queryAttrs...))
		}()
	}

	instrumentedTxFn := func(tx pglib.Tx) error {
		itx := NewTx(tx, &otel.Instrumentation{Tracer: i.tracer})
		return fn(itx)
	}
	return i.inner.ExecInTx(ctx, instrumentedTxFn)
}

func (i *Querier) ExecInTxWithOptions(ctx context.Context, fn func(tx pglib.Tx) error, txOpts pglib.TxOptions) (err error) {
	queryAttrs := queryAttributes(txQueryType)
	ctx, span := otel.StartSpan(ctx, i.tracer, "querier.ExecInTxWithOptions", trace.WithAttributes(queryAttrs...))
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.queryLatency.Record(ctx, int64(time.Since(startTime).Milliseconds()), metric.WithAttributes(queryAttrs...))
		}()
	}

	instrumentedTxFn := func(tx pglib.Tx) error {
		itx := NewTx(tx, &otel.Instrumentation{Tracer: i.tracer})
		return fn(itx)
	}

	return i.inner.ExecInTxWithOptions(ctx, instrumentedTxFn, txOpts)
}

func (i *Querier) Ping(ctx context.Context) (err error) {
	return i.inner.Ping(ctx)
}

func (i *Querier) Close(ctx context.Context) (err error) {
	return i.inner.Close(ctx)
}

func (i *Querier) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (rowCount int64, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "querier.CopyFrom")
	defer otel.CloseSpan(span, err)
	return i.inner.CopyFrom(ctx, tableName, columnNames, srcRows)
}

func (i *Querier) initMetrics() error {
	if i.meter == nil {
		return nil
	}

	var err error
	i.metrics.queryLatency, err = i.meter.Int64Histogram("pgstream.postgres.querier.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of the time taken to perform a query"))
	if err != nil {
		return err
	}

	return nil
}

func queryAttributes(query string) []attribute.KeyValue {
	var qt string
	switch query {
	case "":
		qt = unknownQueryType
	default:
		qt = strings.ToUpper(strings.Split(query, " ")[0])
	}

	attrs := []attribute.KeyValue{
		{
			Key:   queryTypeAttributeKey,
			Value: attribute.StringValue(qt),
		},
	}

	if qt == unknownQueryType {
		return attrs
	}

	return append(attrs, attribute.KeyValue{
		Key:   queryAttributeKey,
		Value: attribute.StringValue(query),
	})
}
