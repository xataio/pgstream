// SPDX-License-Identifier: Apache-2.0

package retrier

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
)

type Querier struct {
	connBuilder     connBuilder
	querier         postgres.Querier
	backoffProvider backoff.Provider
	logger          loglib.Logger
}

type connBuilder func(context.Context) (postgres.Querier, error)

func NewQuerier(ctx context.Context, cfg backoff.Config, connBuilder connBuilder, logger loglib.Logger) (*Querier, error) {
	conn, err := connBuilder(ctx)
	if err != nil {
		return nil, err
	}

	return &Querier{
		connBuilder:     connBuilder,
		querier:         conn,
		backoffProvider: backoff.NewProvider(&cfg),
		logger:          logger,
	}, nil
}

func (q *Querier) Query(ctx context.Context, query string, args ...any) (postgres.Rows, error) {
	var rows postgres.Rows
	var err error
	op := func() error {
		rows, err = q.querier.Query(ctx, query, args...)
		return err
	}

	if err := q.withRetry(ctx, op); err != nil {
		return nil, err
	}
	return rows, nil
}

func (q *Querier) QueryRow(ctx context.Context, dest []any, query string, args ...any) error {
	return q.withRetry(ctx, func() error {
		return q.querier.QueryRow(ctx, dest, query, args...)
	})
}

func (q *Querier) Exec(ctx context.Context, query string, args ...any) (postgres.CommandTag, error) {
	var cmdTag postgres.CommandTag
	var err error
	op := func() error {
		cmdTag, err = q.querier.Exec(ctx, query, args...)
		return err
	}

	if err := q.withRetry(ctx, op); err != nil {
		return postgres.CommandTag{}, err
	}
	return cmdTag, nil
}

func (q *Querier) ExecInTx(ctx context.Context, fn func(tx postgres.Tx) error) error {
	return q.withRetry(ctx, func() error {
		return q.querier.ExecInTx(ctx, fn)
	})
}

func (q *Querier) ExecInTxWithOptions(ctx context.Context, fn func(tx postgres.Tx) error, txOpts postgres.TxOptions) error {
	return q.withRetry(ctx, func() error {
		return q.querier.ExecInTxWithOptions(ctx, fn, txOpts)
	})
}

func (q *Querier) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error) {
	var copyCount int64
	var err error
	op := func() error {
		copyCount, err = q.querier.CopyFrom(ctx, tableName, columnNames, srcRows)
		return err
	}

	if err := q.withRetry(ctx, op); err != nil {
		return 0, err
	}
	return copyCount, nil
}

func (q *Querier) Ping(ctx context.Context) error {
	return q.querier.Ping(ctx)
}

func (q *Querier) Close(ctx context.Context) error {
	return q.querier.Close(ctx)
}

func (q *Querier) withRetry(ctx context.Context, operation func() error) error {
	err := operation()
	if err == nil || !q.isRetriableError(err) {
		return err
	}

	// only initialise the backoff provider if the operation fails
	bo := q.backoffProvider(ctx)
	err = bo.RetryNotify(func() error {
		err = operation()
		if err == nil {
			return nil
		}

		if !q.isRetriableError(err) {
			return fmt.Errorf("%w: %w", err, backoff.ErrPermanent)
		}

		if connErr := q.resetConn(ctx); connErr != nil {
			return fmt.Errorf("unable to reset connection: %w", connErr)
		}

		return err
	}, func(err error, d time.Duration) {
		q.logger.Warn(err, "retrying Postgres operation after error", loglib.Fields{
			"retry_delay": d.String(),
		})
	})

	if err == nil {
		q.logger.Info("retried Postgres operation succeeded")
	}
	return err
}

func (q *Querier) resetConn(ctx context.Context) error {
	conn, connErr := q.connBuilder(ctx)
	if connErr != nil {
		return connErr
	}
	if q.querier != nil {
		q.querier.Close(ctx)
	}
	q.querier = conn
	return nil
}

func (q *Querier) isRetriableError(err error) bool {
	mappedErr := postgres.MapError(err)

	permissionDenied := &postgres.ErrPermissionDenied{}
	constraintViolation := &postgres.ErrConstraintViolation{}
	alreadyExists := &postgres.ErrRelationAlreadyExists{}
	ruleViolation := &postgres.ErrRuleViolation{}
	syntaxError := &postgres.ErrSyntaxError{}
	doesNotExist := &postgres.ErrRelationDoesNotExist{}
	programLimitExceeded := &postgres.ErrProgramLimitExceeded{}
	switch {
	case errors.As(mappedErr, &permissionDenied),
		errors.As(mappedErr, &constraintViolation),
		errors.As(mappedErr, &alreadyExists),
		errors.As(mappedErr, &ruleViolation),
		errors.As(mappedErr, &syntaxError),
		errors.As(mappedErr, &doesNotExist),
		errors.As(mappedErr, &programLimitExceeded):
		return false
	}

	// for now retry errors that are not context cancellation
	return !errors.Is(err, context.Canceled)
}
