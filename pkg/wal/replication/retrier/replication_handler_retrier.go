// SPDX-License-Identifier: Apache-2.0

package retrier

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

type HandlerRetrier struct {
	inner           replication.Handler
	backoffProvider backoff.Provider
	logger          loglib.Logger
}

type Option func(*HandlerRetrier)

const (
	defaultInitialInterval = 500 * time.Millisecond
	defaultMaxInterval     = 30 * time.Second
)

func NewHandler(h replication.Handler, backoffConfig backoff.Config, opts ...Option) *HandlerRetrier {
	if !backoffConfig.IsSet() {
		backoffConfig = defaultBackoffConfig()
	}

	hr := &HandlerRetrier{
		inner:           h,
		backoffProvider: backoff.NewProvider(&backoffConfig),
		logger:          loglib.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(hr)
	}

	return hr
}

func WithLogger(logger loglib.Logger) Option {
	return func(hr *HandlerRetrier) {
		hr.logger = logger
	}
}

func (h *HandlerRetrier) StartReplication(ctx context.Context) error {
	return h.inner.StartReplication(ctx)
}

func (h *HandlerRetrier) StartReplicationFromLSN(ctx context.Context, lsn replication.LSN) error {
	return h.inner.StartReplicationFromLSN(ctx, lsn)
}

func (h *HandlerRetrier) ReceiveMessage(ctx context.Context) (*replication.Message, error) {
	var msg *replication.Message
	var err error
	op := func() error {
		msg, err = h.inner.ReceiveMessage(ctx)
		return err
	}

	if err := h.withRetry(ctx, op); err != nil {
		return nil, err
	}
	return msg, nil
}

func (h *HandlerRetrier) SyncLSN(ctx context.Context, lsn replication.LSN) error {
	return h.withRetry(ctx, func() error {
		return h.inner.SyncLSN(ctx, lsn)
	})
}

func (h *HandlerRetrier) GetReplicationLag(ctx context.Context) (int64, error) {
	return h.inner.GetReplicationLag(ctx)
}

func (h *HandlerRetrier) GetCurrentLSN(ctx context.Context) (replication.LSN, error) {
	return h.inner.GetCurrentLSN(ctx)
}

func (h *HandlerRetrier) ResetConnection(ctx context.Context) error {
	return h.inner.ResetConnection(ctx)
}

func (h *HandlerRetrier) GetLSNParser() replication.LSNParser {
	return h.inner.GetLSNParser()
}

func (h *HandlerRetrier) GetReplicationSlotName() string {
	return h.inner.GetReplicationSlotName()
}

func (h *HandlerRetrier) Close() error {
	return h.inner.Close()
}

func (h *HandlerRetrier) withRetry(ctx context.Context, operation func() error) error {
	err := operation()
	if err == nil || !h.isRetriableError(err) {
		return err
	}

	bo := h.backoffProvider(ctx)
	err = bo.RetryNotify(func() error {
		err = operation()
		if err == nil {
			return nil
		}

		if !h.isRetriableError(err) {
			return fmt.Errorf("%w: %w", err, backoff.ErrPermanent)
		}

		if connErr := h.inner.ResetConnection(ctx); connErr != nil {
			return fmt.Errorf("unable to reset connection: %w", connErr)
		}

		return err
	}, func(err error, d time.Duration) {
		h.logger.Warn(err, "retrying replication handler operation after error", loglib.Fields{
			"retry_delay": d.String(),
		})
	})

	if err == nil {
		h.logger.Info("retried replication handler operation succeeded")
	}
	return err
}

func (h *HandlerRetrier) isRetriableError(err error) bool {
	// for now retry errors that are not context cancellation
	return !errors.Is(err, context.Canceled)
}

func defaultBackoffConfig() backoff.Config {
	return backoff.Config{
		Exponential: &backoff.ExponentialConfig{
			InitialInterval: defaultInitialInterval,
			MaxInterval:     defaultMaxInterval,
		},
	}
}
