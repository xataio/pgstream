// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/transformers"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Transformer struct {
	inner   transformers.Transformer
	tracer  trace.Tracer
	meter   metric.Meter
	metrics *metrics
}

type metrics struct {
	transformLatency metric.Int64Histogram
}

const typeAttributeKey = "transformer_type"

func NewTransformer(t transformers.Transformer, instrumentation *otel.Instrumentation) (transformers.Transformer, error) {
	if instrumentation == nil {
		return t, nil
	}

	transformer := &Transformer{
		inner:   t,
		tracer:  instrumentation.Tracer,
		meter:   instrumentation.Meter,
		metrics: &metrics{},
	}

	if err := transformer.initMetrics(); err != nil {
		return nil, fmt.Errorf("initialising transformer metrics: %w", err)
	}

	return transformer, nil
}

func (i *Transformer) Transform(ctx context.Context, v transformers.Value) (res any, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "transformer.Transform", trace.WithAttributes(i.typeAttribute()))
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.transformLatency.Record(ctx, int64(time.Since(startTime).Milliseconds()), metric.WithAttributes(i.typeAttribute()))
		}()
	}
	return i.inner.Transform(ctx, v)
}

func (i *Transformer) CompatibleTypes() []transformers.SupportedDataType {
	return i.inner.CompatibleTypes()
}

func (i *Transformer) Type() transformers.TransformerType {
	return i.inner.Type()
}

func (i *Transformer) Close() error {
	return i.inner.Close()
}

func (i *Transformer) initMetrics() error {
	if i.meter == nil {
		return nil
	}

	var err error
	i.metrics.transformLatency, err = i.meter.Int64Histogram("pgstream.transformer.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of the time taken to transform the value"))
	if err != nil {
		return err
	}

	return nil
}

func (i *Transformer) typeAttribute() attribute.KeyValue {
	return attribute.KeyValue{
		Key:   typeAttributeKey,
		Value: attribute.StringValue(string(i.inner.Type())),
	}
}
