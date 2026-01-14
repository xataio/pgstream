// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal/replication"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Handler struct {
	inner           replication.Handler
	meter           metric.Meter
	tracer          trace.Tracer
	metrics         *metrics
	metricRetriever metricRetriever
}

type metrics struct {
	replicationLag metric.Int64ObservableGauge
}

type metricRetriever interface {
	GetReplicationLag(ctx context.Context) (int64, error)
}

func NewHandler(inner replication.Handler, instrumentation *otel.Instrumentation) (replication.Handler, error) {
	if instrumentation == nil {
		return inner, nil
	}

	h := &Handler{
		inner:           inner,
		meter:           instrumentation.Meter,
		tracer:          instrumentation.Tracer,
		metrics:         &metrics{},
		metricRetriever: newMetricsCache(inner, defaultCacheTTL),
	}

	if err := h.initMetrics(); err != nil {
		return nil, fmt.Errorf("error initialising replication handler metrics: %w", err)
	}

	return h, nil
}

func (h *Handler) StartReplication(ctx context.Context) error {
	return h.inner.StartReplication(ctx)
}

func (h *Handler) StartReplicationFromLSN(ctx context.Context, lsn replication.LSN) error {
	return h.inner.StartReplicationFromLSN(ctx, lsn)
}

func (h *Handler) ReceiveMessage(ctx context.Context) (msg *replication.Message, err error) {
	ctx, span := otel.StartSpan(ctx, h.tracer, "replicationhandler.ReceiveMessage")
	defer otel.CloseSpan(span, err)
	return h.inner.ReceiveMessage(ctx)
}

func (h *Handler) SyncLSN(ctx context.Context, lsn replication.LSN) (err error) {
	ctx, span := otel.StartSpan(ctx, h.tracer, "replicationhandler.SyncLSN")
	defer otel.CloseSpan(span, err)
	return h.inner.SyncLSN(ctx, lsn)
}

func (h *Handler) GetReplicationLag(ctx context.Context) (int64, error) {
	return h.inner.GetReplicationLag(ctx)
}

func (h *Handler) GetCurrentLSN(ctx context.Context) (replication.LSN, error) {
	return h.inner.GetCurrentLSN(ctx)
}

func (h *Handler) GetLSNParser() replication.LSNParser {
	return h.inner.GetLSNParser()
}

func (h *Handler) GetReplicationSlotName() string {
	return h.inner.GetReplicationSlotName()
}

func (h *Handler) ResetConnection(ctx context.Context) error {
	return h.inner.ResetConnection(ctx)
}

func (h *Handler) Close() error {
	return h.inner.Close()
}

func (h *Handler) initMetrics() error {
	if h.meter == nil {
		return nil
	}

	var err error
	h.metrics.replicationLag, err = h.meter.Int64ObservableGauge("pgstream.replication.lag",
		metric.WithUnit("bytes"),
		metric.WithDescription("Replication lag in bytes accrued by the WAL consumer"))
	if err != nil {
		return err
	}

	observe := func(ctx context.Context, o metric.Observer) error {
		replicationLag, err := h.metricRetriever.GetReplicationLag(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(h.metrics.replicationLag, replicationLag,
			metric.WithAttributes(attribute.String("replication_slot", h.inner.GetReplicationSlotName())))
		return nil
	}

	_, err = h.meter.RegisterCallback(
		observe,
		h.metrics.replicationLag,
	)
	if err != nil {
		return fmt.Errorf("registering replication handler metric callbacks: %w", err)
	}

	return nil
}
