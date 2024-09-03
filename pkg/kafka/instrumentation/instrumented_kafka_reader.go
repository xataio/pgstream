// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/otel"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Reader struct {
	inner   kafka.MessageReader
	meter   metric.Meter
	tracer  trace.Tracer
	metrics *readerMetrics
}

type readerMetrics struct {
	msgBytes        metric.Int64Histogram
	fetchLatency    metric.Int64Histogram
	commitLatency   metric.Int64Histogram
	commitBatchSize metric.Int64Histogram
}

func NewReader(inner kafka.MessageReader, instrumentation *otel.Instrumentation) (kafka.MessageReader, error) {
	if instrumentation == nil {
		return inner, nil
	}

	i := &Reader{
		inner:   inner,
		meter:   instrumentation.Meter,
		tracer:  instrumentation.Tracer,
		metrics: &readerMetrics{},
	}

	if err := i.initMetrics(); err != nil {
		return nil, fmt.Errorf("error initialising kafka reader metrics: %w", err)
	}

	return i, nil
}

func (i *Reader) initMetrics() error {
	if i.meter == nil {
		return nil
	}

	var err error
	i.metrics.msgBytes, err = i.meter.Int64Histogram("pgstream.kafka.reader.msg.bytes",
		metric.WithUnit("bytes"),
		metric.WithDescription("Distribution of message bytes read by the kafka reader"))
	if err != nil {
		return err
	}

	i.metrics.fetchLatency, err = i.meter.Int64Histogram("pgstream.kafka.reader.fetch.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of time taken by the reader to fetch messages from kafka"))
	if err != nil {
		return err
	}

	i.metrics.commitLatency, err = i.meter.Int64Histogram("pgstream.kafka.reader.commit.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of time taken by the reader to commit messages to kafka"))
	if err != nil {
		return err
	}

	i.metrics.commitBatchSize, err = i.meter.Int64Histogram("pgstream.kafka.reader.commit.batch.size",
		metric.WithUnit("offsets"),
		metric.WithDescription("Distribution of the offset batch size committed by the kafka reader"))
	if err != nil {
		return err
	}

	return nil
}

func (i *Reader) FetchMessage(ctx context.Context) (msg *kafka.Message, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "kafka.FetchMessages")
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.fetchLatency.Record(ctx, time.Since(startTime).Milliseconds())
		}()

	}

	msg, err = i.inner.FetchMessage(ctx)
	if msg != nil && i.meter != nil {
		i.metrics.msgBytes.Record(ctx, int64(len(msg.Value)))
	}

	return msg, err
}

func (i *Reader) CommitOffsets(ctx context.Context, offsets ...*kafka.Offset) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "kafka.CommitOffsets")
	defer otel.CloseSpan(span, err)

	if i.meter != nil {
		startTime := time.Now()
		defer func() {
			i.metrics.commitLatency.Record(ctx, time.Since(startTime).Milliseconds())
		}()
		i.metrics.commitBatchSize.Record(ctx, int64(len(offsets)))
	}

	return i.inner.CommitOffsets(ctx, offsets...)
}

func (i *Reader) Close() error {
	return i.inner.Close()
}
