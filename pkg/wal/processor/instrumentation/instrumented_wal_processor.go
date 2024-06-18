// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"

	"go.opentelemetry.io/otel/metric"
)

type Processor struct {
	inner   processor.Processor
	meter   metric.Meter
	metrics *metrics
}

type metrics struct {
	processLag        metric.Int64Histogram
	processingLatency metric.Int64Histogram
}

func NewProcessor(p processor.Processor, meter metric.Meter) (*Processor, error) {
	processor := &Processor{
		inner:   p,
		metrics: &metrics{},
		meter:   meter,
	}

	if err := processor.initMetrics(); err != nil {
		return nil, fmt.Errorf("initialising processor metrics: %w", err)
	}

	return processor, nil
}

func (i *Processor) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	startTime := time.Now()
	defer func() {
		i.metrics.processingLatency.Record(ctx, int64(time.Since(startTime).Milliseconds()))
	}()

	if event.Data != nil {
		timestamp, err := event.Data.GetTimestamp()
		if err == nil {
			i.metrics.processLag.Record(ctx, time.Since(timestamp).Milliseconds())
		}
	}
	return i.inner.ProcessWALEvent(ctx, event)
}

func (i *Processor) Name() string {
	return i.inner.Name()
}

func (i *Processor) initMetrics() error {
	var err error
	i.metrics.processLag, err = i.meter.Int64Histogram(fmt.Sprintf("pgstream.%s.processing.lag", i.inner.Name()),
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of time passed since the wal event was put into the stream until it is processed by the wal event processor"))
	if err != nil {
		return err
	}

	i.metrics.processingLatency, err = i.meter.Int64Histogram(fmt.Sprintf("pgstream.%s.processing.latency", i.inner.Name()),
		metric.WithUnit("ms"),
		metric.WithDescription("Distribution of the time taken to process the wal event"))
	if err != nil {
		return err
	}

	return nil
}
