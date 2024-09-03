// SPDX-License-Identifier: Apache-2.0

package otel

import (
	"context"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Instrumentation struct {
	Meter  metric.Meter
	Tracer trace.Tracer
}

func (i *Instrumentation) IsEnabled() bool {
	return i != nil && (i.Meter != nil || i.Tracer != nil)
}

// StartSpan will start a span using the tracer on input. If the tracer is nil,
// the context returned is the same as on input, and the span will be nil.
func StartSpan(ctx context.Context, tracer trace.Tracer, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if tracer == nil {
		return ctx, nil
	}
	return tracer.Start(ctx, name, opts...)
}

// CloseSpan closes a span and records the given error if not nil. If the span
// is nil, this is a noop.
func CloseSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	recordSpanResult(span, err)
	span.End()
}

func recordSpanResult(span trace.Span, err error) {
	if err == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, "")
}
