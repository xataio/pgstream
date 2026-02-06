// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"
	"time"

	natslib "github.com/xataio/pgstream/pkg/nats"
	"github.com/xataio/pgstream/pkg/otel"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Writer struct {
	inner   natslib.MessageWriter
	meter   metric.Meter
	tracer  trace.Tracer
	metrics *writerMetrics
}

type writerMetrics struct {
	batchSize    metric.Int64Histogram
	batchBytes   metric.Int64Histogram
	writeLatency metric.Int64Histogram
}

func NewWriter(inner natslib.MessageWriter, instrumentation *otel.Instrumentation) (natslib.MessageWriter, error) {
	if instrumentation == nil {
		return inner, nil
	}

	i := &Writer{
		inner:   inner,
		meter:   instrumentation.Meter,
		tracer:  instrumentation.Tracer,
		metrics: &writerMetrics{},
	}

	if err := i.initMetrics(); err != nil {
		return nil, fmt.Errorf("error initialising nats writer metrics: %w", err)
	}

	return i, nil
}

func (i *Writer) initMetrics() error {
	if i.meter == nil {
		return nil
	}

	var err error
	i.metrics.batchSize, err = i.meter.Int64Histogram("pgstream.nats.writer.batch.size",
		metric.WithUnit("messages"),
		metric.WithDescription("Distribution of message batch size written by the nats jetstream writer"))
	if err != nil {
		return err
	}

	i.metrics.batchBytes, err = i.meter.Int64Histogram("pgstream.nats.writer.batch.bytes",
		metric.WithUnit("bytes"),
		metric.WithDescription("Distribution of message batch bytes written by the nats jetstream writer"))
	if err != nil {
		return err
	}

	i.metrics.writeLatency, err = i.meter.Int64Histogram("pgstream.nats.writer.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of time taken by the writer to send messages to nats jetstream"))
	if err != nil {
		return err
	}

	return nil
}

func (i *Writer) WriteMessages(ctx context.Context, msgs ...natslib.Message) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "nats.WriteMessages")
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.writeLatency.Record(ctx, time.Since(startTime).Milliseconds())
		}()
		i.metrics.batchSize.Record(ctx, int64(len(msgs)))

		go func() {
			batchBytes := 0
			for _, msg := range msgs {
				batchBytes += len(msg.Value)
			}
			i.metrics.batchBytes.Record(ctx, int64(batchBytes))
		}()
	}

	return i.inner.WriteMessages(ctx, msgs...)
}

func (i *Writer) Close() error {
	return i.inner.Close()
}
