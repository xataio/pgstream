// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"fmt"
	"sync/atomic"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DroppedCounter accumulates what ignore_send_errors has silently discarded.
type DroppedCounter struct {
	batches  atomic.Uint64
	messages atomic.Uint64
}

func NewDroppedCounter() *DroppedCounter {
	return &DroppedCounter{}
}

func (c *DroppedCounter) record(messages uint64) (uint64, uint64) {
	if c == nil {
		return 0, 0
	}
	return c.batches.Add(1), c.messages.Add(messages)
}

// Batches returns the number of batches silently discarded because
// ignore_send_errors is enabled. It stays at zero otherwise.
func (c *DroppedCounter) Batches() uint64 {
	if c == nil {
		return 0
	}
	return c.batches.Load()
}

// Messages returns the number of messages lost with the batches counted by
// Batches.
func (c *DroppedCounter) Messages() uint64 {
	if c == nil {
		return 0
	}
	return c.messages.Load()
}

// LogTotals reports what was dropped, once, at shutdown.
func (c *DroppedCounter) LogTotals(logger loglib.Logger) {
	batches := c.Batches()
	if batches == 0 {
		return
	}
	logger.Error(nil, "closed with dropped batches", loglib.Fields{
		"severity":         "DATALOSS",
		"dropped_batches":  batches,
		"dropped_messages": c.Messages(),
	})
}

const (
	droppedBatchesMetricName  = "pgstream.batch.sender.dropped_batches"
	droppedMessagesMetricName = "pgstream.batch.sender.dropped_messages"
)

// RegisterMetrics exports the counter as two observable counters, labelled with
// the writer that owns it.
func (c *DroppedCounter) RegisterMetrics(i *otel.Instrumentation, writerType string) error {
	if i == nil || i.Meter == nil {
		return nil
	}
	meter := i.Meter

	droppedBatches, err := meter.Int64ObservableCounter(droppedBatchesMetricName,
		metric.WithUnit("{batch}"),
		metric.WithDescription("Number of batches silently dropped by the batch sender because ignore_send_errors is enabled"))
	if err != nil {
		return fmt.Errorf("creating %s counter: %w", droppedBatchesMetricName, err)
	}

	droppedMessages, err := meter.Int64ObservableCounter(droppedMessagesMetricName,
		metric.WithUnit("{message}"),
		metric.WithDescription("Number of messages lost with the batches counted by "+droppedBatchesMetricName))
	if err != nil {
		return fmt.Errorf("creating %s counter: %w", droppedMessagesMetricName, err)
	}

	_, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			attrs := metric.WithAttributes(attribute.String("writer_type", writerType))
			o.ObserveInt64(droppedBatches, int64(c.Batches()), attrs)
			o.ObserveInt64(droppedMessages, int64(c.Messages()), attrs)
			return nil
		},
		droppedBatches, droppedMessages,
	)
	if err != nil {
		return fmt.Errorf("registering batch sender dropped counters: %w", err)
	}

	return nil
}
