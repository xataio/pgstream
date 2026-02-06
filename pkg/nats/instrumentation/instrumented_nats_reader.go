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

type Reader struct {
	inner   natslib.MessageReader
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

func NewReader(inner natslib.MessageReader, instrumentation *otel.Instrumentation) (natslib.MessageReader, error) {
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
		return nil, fmt.Errorf("error initialising nats reader metrics: %w", err)
	}

	return i, nil
}

func (i *Reader) initMetrics() error {
	if i.meter == nil {
		return nil
	}

	var err error
	i.metrics.msgBytes, err = i.meter.Int64Histogram("pgstream.nats.reader.msg.bytes",
		metric.WithUnit("bytes"),
		metric.WithDescription("Distribution of message bytes read by the nats jetstream reader"))
	if err != nil {
		return err
	}

	i.metrics.fetchLatency, err = i.meter.Int64Histogram("pgstream.nats.reader.fetch.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of time taken by the reader to fetch messages from nats jetstream"))
	if err != nil {
		return err
	}

	i.metrics.commitLatency, err = i.meter.Int64Histogram("pgstream.nats.reader.commit.latency",
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of time taken by the reader to commit messages to nats jetstream"))
	if err != nil {
		return err
	}

	i.metrics.commitBatchSize, err = i.meter.Int64Histogram("pgstream.nats.reader.commit.batch.size",
		metric.WithUnit("offsets"),
		metric.WithDescription("Distribution of the offset batch size committed by the nats jetstream reader"))
	if err != nil {
		return err
	}

	return nil
}

func (i *Reader) FetchMessage(ctx context.Context) (msg *natslib.Message, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "nats.FetchMessages")
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

func (i *Reader) CommitOffsets(ctx context.Context, offsets ...*natslib.Offset) (err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "nats.CommitOffsets")
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
