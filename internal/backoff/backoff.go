// SPDX-License-Identifier: Apache-2.0

package backoff

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type Backoff interface {
	RetryNotify(Operation, Notify) error
	Retry(Operation) error
}

type (
	Operation func() error
	Notify    func(error, time.Duration)
)

type Config struct {
	Exponential *ExponentialConfig
	Constant    *ConstantConfig
}

type ExponentialConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxRetries      uint
}

type ConstantConfig struct {
	Interval   time.Duration
	MaxRetries uint
}

// ExponentialBackoff is a wrapper around the cenkalti exponential backoff
type ExponentialBackoff struct {
	backoff.BackOff
}

var ErrPermanent = errors.New("permanent error, do not retry")

type Provider func(ctx context.Context) Backoff

// NewProvider returns a backoff provider based on the config on input. If no
// valid input is provided, a no retry backoff provider is returned instead.
func NewProvider(cfg *Config) Provider {
	switch {
	case cfg.Constant != nil:
		return func(ctx context.Context) Backoff {
			return NewConstantBackoff(ctx, cfg.Constant)
		}
	case cfg.Exponential != nil:
		return func(ctx context.Context) Backoff {
			return NewExponentialBackoff(ctx, cfg.Exponential)
		}
	default:
		return func(ctx context.Context) Backoff {
			return NewStopBackoff()
		}
	}
}

func NewExponentialBackoff(ctx context.Context, cfg *ExponentialConfig) *ExponentialBackoff {
	exp := backoff.NewExponentialBackOff()
	exp.InitialInterval = cfg.InitialInterval
	exp.MaxElapsedTime = cfg.MaxInterval

	var bo backoff.BackOff = exp
	if cfg.MaxRetries > 0 {
		bo = backoff.WithMaxRetries(bo, uint64(cfg.MaxRetries))
	}
	bo = backoff.WithContext(bo, ctx)

	return &ExponentialBackoff{
		BackOff: bo,
	}
}

func (ebo *ExponentialBackoff) Retry(op Operation) error {
	return retryNotify(ebo, op, nil)
}

func (ebo *ExponentialBackoff) RetryNotify(op Operation, notify Notify) error {
	return retryNotify(ebo, op, notify)
}

type ConstantBackoff struct {
	backoff.BackOff
}

func NewConstantBackoff(ctx context.Context, cfg *ConstantConfig) *ConstantBackoff {
	exp := backoff.NewConstantBackOff(cfg.Interval)
	var bo backoff.BackOff = exp
	if cfg.MaxRetries > 0 {
		bo = backoff.WithMaxRetries(bo, uint64(cfg.MaxRetries))
	}
	bo = backoff.WithContext(bo, ctx)

	return &ConstantBackoff{
		BackOff: bo,
	}
}

func (cbo *ConstantBackoff) Retry(op Operation) error {
	return retryNotify(cbo, op, nil)
}

func (cbo *ConstantBackoff) RetryNotify(op Operation, notify Notify) error {
	return retryNotify(cbo, op, notify)
}

type StopBackoff struct {
	backoff.BackOff
}

func NewStopBackoff() *StopBackoff {
	return &StopBackoff{
		BackOff: &backoff.StopBackOff{},
	}
}

func (sbo *StopBackoff) Retry(op Operation) error {
	return retryNotify(sbo, op, nil)
}

func (sbo *StopBackoff) RetryNotify(op Operation, notify Notify) error {
	return retryNotify(sbo, op, notify)
}

func retryNotify(b backoff.BackOff, op Operation, notify Notify) error {
	boOp := func() error {
		err := op()
		if errors.Is(err, ErrPermanent) {
			return backoff.Permanent(err)
		}
		return err
	}
	return backoff.RetryNotify(boOp, b, backoff.Notify(notify))
}
