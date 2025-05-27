// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Processor struct {
	inner      processor.Processor
	tracer     trace.Tracer
	meter      metric.Meter
	metrics    *metrics
	targetType string
}

type metrics struct {
	processLag        metric.Int64Histogram
	processingLatency metric.Int64Histogram
}

const targetAttributeKey = "target"

func NewProcessor(p processor.Processor, instrumentation *otel.Instrumentation) (processor.Processor, error) {
	if instrumentation == nil {
		return p, nil
	}

	processor := &Processor{
		inner:      p,
		tracer:     instrumentation.Tracer,
		meter:      instrumentation.Meter,
		metrics:    &metrics{},
		targetType: p.Name(),
	}

	if err := processor.initMetrics(); err != nil {
		return nil, fmt.Errorf("initialising processor metrics: %w", err)
	}

	return processor, nil
}

func (i *Processor) ProcessWALEvent(ctx context.Context, event *wal.Event) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "processor.ProcessWALEvent", trace.WithAttributes(i.targetAttribute()))
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.processingLatency.Record(ctx, int64(time.Since(startTime).Nanoseconds()), metric.WithAttributes(i.targetAttribute()))
		}()

		if event.Data != nil {
			timestamp, err := event.Data.GetTimestamp()
			if err == nil {
				i.metrics.processLag.Record(ctx, time.Since(timestamp).Nanoseconds(), metric.WithAttributes(i.targetAttribute()))
			}
		}
	}
	return i.inner.ProcessWALEvent(ctx, event)
}

func (i *Processor) Name() string {
	return i.inner.Name()
}

func (i *Processor) Close() error {
	return i.inner.Close()
}

func (i *Processor) initMetrics() error {
	if i.meter == nil {
		return nil
	}

	var err error
	i.metrics.processLag, err = i.meter.Int64Histogram("pgstream.target.processing.lag",
		metric.WithUnit("ns"),
		metric.WithDescription("Distribution of time passed since the wal event was put into the stream until it is processed by the wal event processor"))
	if err != nil {
		return err
	}

	i.metrics.processingLatency, err = i.meter.Int64Histogram("pgstream.target.processing.latency",
		metric.WithUnit("ns"),
		metric.WithDescription("Distribution of the time taken to process the wal event"))
	if err != nil {
		return err
	}

	return nil
}

func (i *Processor) targetAttribute() attribute.KeyValue {
	return attribute.KeyValue{
		Key:   targetAttributeKey,
		Value: attribute.StringValue(i.targetType),
	}
}
