// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/kafka"

	"go.opentelemetry.io/otel/metric"
)

type Writer struct {
	inner   kafka.MessageWriter
	meter   metric.Meter
	metrics *writerMetrics
}

type writerMetrics struct {
	batchSize    metric.Int64Histogram
	batchBytes   metric.Int64Histogram
	writeLatency metric.Int64Histogram
}

func NewWriter(inner kafka.MessageWriter, meter metric.Meter) (*Writer, error) {
	i := &Writer{
		inner:   inner,
		meter:   meter,
		metrics: &writerMetrics{},
	}

	if err := i.initMetrics(); err != nil {
		return nil, fmt.Errorf("error initialising kafka writer metrics: %w", err)
	}

	return i, nil
}

func (i *Writer) initMetrics() error {
	var err error
	i.metrics.batchSize, err = i.meter.Int64Histogram("pgstream.kafka.writer.batch.size",
		metric.WithUnit("messages"),
		metric.WithDescription("Distribution of message batch size written by the kafka writer"))
	if err != nil {
		return err
	}

	i.metrics.batchBytes, err = i.meter.Int64Histogram("pgstream.kafka.writer.batch.bytes",
		metric.WithUnit("bytes"),
		metric.WithDescription("Distribution of message batch bytes written by the kafka writer"))
	if err != nil {
		return err
	}

	i.metrics.writeLatency, err = i.meter.Int64Histogram("pgstream.kafka.writer.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of time taken by the writer to send messages to kafka"))
	if err != nil {
		return err
	}

	return nil
}

func (i *Writer) WriteMessages(ctx context.Context, msgs ...kafka.Message) (err error) {
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

	return i.inner.WriteMessages(ctx, msgs...)
}

func (i *Writer) Close() error {
	return i.inner.Close()
}
