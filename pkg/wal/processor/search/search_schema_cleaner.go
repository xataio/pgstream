// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/internal/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
)

type cleaner interface {
	deleteSchema(context.Context, string) error
	start(context.Context)
	stop()
}

type store interface {
	DeleteSchema(ctx context.Context, schemaName string) error
}

// schemaCleaner takes care of deleting schemas from the search store
// asynchronously
type schemaCleaner struct {
	logger              loglib.Logger
	deleteSchemaQueue   chan string
	store               store
	backoffProvider     backoff.Provider
	registrationTimeout time.Duration
}

const (
	maxDeleteQueueSize         = 5000
	defaultRegistrationTimeout = 5 * time.Second
)

var errRegistrationTimeout = errors.New("timeout registering schema for clean up")

func newSchemaCleaner(cfg *backoff.Config, store store, logger loglib.Logger) *schemaCleaner {
	return &schemaCleaner{
		logger:              logger,
		deleteSchemaQueue:   make(chan string, maxDeleteQueueSize),
		store:               store,
		registrationTimeout: defaultRegistrationTimeout,
		backoffProvider:     backoff.NewProvider(cfg),
	}
}

// deleteSchema writes a delete schema item to the delete queue. Times out and returns an error after 5 seconds.
func (sc *schemaCleaner) deleteSchema(_ context.Context, schemaName string) error {
	select {
	case sc.deleteSchemaQueue <- schemaName:
		return nil
	case <-time.After(sc.registrationTimeout):
		return errRegistrationTimeout
	}
}

// start will continuously process schema items from the local delete queue
func (sc *schemaCleaner) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case schema := <-sc.deleteSchemaQueue:
			bo := sc.backoffProvider(ctx)
			err := bo.RetryNotify(
				func() error {
					return getRetryError(sc.store.DeleteSchema(ctx, schema))
				},
				func(err error, duration time.Duration) {
					sc.logger.Warn(err, "search schema cleaner: delete schema retry failed", loglib.Fields{
						"backoff": duration,
						"schema":  schema,
					})
				})
			if err != nil {
				sc.logger.Error(err, "search schema cleaner: delete schema", loglib.Fields{"schema": schema})
			}
		}
	}
}

// stop will stop the processing of delete items from the queue and release
// internal resources
func (sc schemaCleaner) stop() {
	close(sc.deleteSchemaQueue)
}

// getRetryError returns a backoff permanent error if the given error is not
// retryable
func getRetryError(err error) error {
	if err != nil {
		if errors.Is(err, ErrRetriable) {
			return err
		}
		return fmt.Errorf("%w: %w", err, backoff.ErrPermanent)
	}
	return nil
}
