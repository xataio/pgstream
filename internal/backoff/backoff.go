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
}

type (
	Operation func() error
	Notify    func(error, time.Duration)
)

type Config struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxRetries      uint
}

// ExponentialBackoff is a wrapper around the cenkalti exponential backoff
type ExponentialBackoff struct {
	backoff.BackOff
}

var ErrPermanent = errors.New("permanent error, do not retry")

type Provider func(ctx context.Context) Backoff

func NewExponentialBackoff(ctx context.Context, cfg *Config) *ExponentialBackoff {
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

func (ebo *ExponentialBackoff) RetryNotify(op Operation, notify Notify) error {
	boOp := func() error {
		err := op()
		if errors.Is(err, ErrPermanent) {
			return backoff.Permanent(err)
		}
		return err
	}
	return backoff.RetryNotify(boOp, ebo, backoff.Notify(notify))
}
