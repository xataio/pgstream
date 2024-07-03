// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/pkg/wal/replication"

	"go.opentelemetry.io/otel/metric"
)

type Handler struct {
	inner   replication.Handler
	meter   metric.Meter
	metrics *metrics
}

type metrics struct {
	replicationLag metric.Int64ObservableGauge
}

func NewHandler(inner replication.Handler, meter metric.Meter) (*Handler, error) {
	h := &Handler{
		inner:   inner,
		meter:   meter,
		metrics: &metrics{},
	}

	if err := h.initMetrics(); err != nil {
		return nil, fmt.Errorf("error initialising replication handler metrics: %w", err)
	}

	return h, nil
}

func (h *Handler) StartReplication(ctx context.Context) error {
	return h.inner.StartReplication(ctx)
}

func (h *Handler) ReceiveMessage(ctx context.Context) (*replication.Message, error) {
	return h.inner.ReceiveMessage(ctx)
}

func (h *Handler) SyncLSN(ctx context.Context, lsn replication.LSN) (err error) {
	return h.inner.SyncLSN(ctx, lsn)
}

func (h *Handler) GetReplicationLag(ctx context.Context) (int64, error) {
	return h.inner.GetReplicationLag(ctx)
}

func (h *Handler) GetLSNParser() replication.LSNParser {
	return h.inner.GetLSNParser()
}

func (h *Handler) Close() error {
	return h.inner.Close()
}

func (h *Handler) initMetrics() error {
	var err error
	h.metrics.replicationLag, err = h.meter.Int64ObservableGauge("pgstream.replication.lag",
		metric.WithUnit("bytes"),
		metric.WithDescription("Replication lag in bytes accrued by the WAL consumer"))
	if err != nil {
		return err
	}

	observe := func(ctx context.Context, o metric.Observer) error {
		replicationLag, err := h.inner.GetReplicationLag(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(h.metrics.replicationLag, replicationLag)
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
