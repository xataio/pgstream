// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
)

// StoreRetrier applies a retry strategy to failed search store operations.
type StoreRetrier struct {
	inner           Store
	logger          loglib.Logger
	backoffProvider backoff.Provider
	marshaler       func(any) ([]byte, error)
}

type StoreRetryConfig struct {
	// If not provided it defaults to using exponential backoff with initial
	// interval of 1s, max interval of 1min, and 0 max retries.
	Backoff backoff.Config
}

type StoreOption func(*StoreRetrier)

const (
	defaultStoreRetryInitialInterval = time.Second
	defaultStoreRetryMaxInterval     = time.Minute
	defaultStoreRetryMaxRetries      = 0
)

var errPartialDocumentSend = errors.New("failed to send some or all documents")

func NewStoreRetrier(s Store, cfg StoreRetryConfig, opts ...StoreOption) *StoreRetrier {
	sr := &StoreRetrier{
		inner:           s,
		logger:          loglib.NewNoopLogger(),
		backoffProvider: backoff.NewProvider(cfg.backoffConfig()),
		marshaler:       json.Marshal,
	}

	for _, opt := range opts {
		opt(sr)
	}

	return sr
}

func WithStoreLogger(logger loglib.Logger) StoreOption {
	return func(sr *StoreRetrier) {
		sr.logger = logger
	}
}

func (s *StoreRetrier) GetMapper() Mapper {
	return s.inner.GetMapper()
}

func (s *StoreRetrier) ApplySchemaChange(ctx context.Context, logEntry *schemalog.LogEntry) error {
	return s.inner.ApplySchemaChange(ctx, logEntry)
}

func (s *StoreRetrier) DeleteSchema(ctx context.Context, schemaName string) error {
	bo := s.backoffProvider(ctx)
	err := bo.RetryNotify(
		func() error {
			return getRetryError(s.inner.DeleteSchema(ctx, schemaName))
		},
		func(err error, duration time.Duration) {
			s.logger.Warn(err, "delete schema retry failed", loglib.Fields{
				"backoff": duration,
				"schema":  schemaName,
			})
		})
	if err != nil {
		s.logger.Error(err, "delete schema", loglib.Fields{"schema": schemaName})
	}
	return err
}

func (s *StoreRetrier) DeleteTableDocuments(ctx context.Context, schemaName string, tableIDs []string) error {
	return s.inner.DeleteTableDocuments(ctx, schemaName, tableIDs)
}

// SendDocuments will go over failed documents, identifying any with retriable
// errors and retrying them with the configured backoff policy.
func (s *StoreRetrier) SendDocuments(ctx context.Context, docs []Document) ([]DocumentError, error) {
	docsToSend := docs
	failedDocs := []DocumentError{}
	docsDropped := []DocumentError{}
	send := func(ctx context.Context) error {
		total := len(docsToSend)
		var err error
		failedDocs, err = s.inner.SendDocuments(ctx, docsToSend)
		if err != nil {
			return err
		}

		var dropped []DocumentError
		docsToSend, dropped = s.getRetriableDocs(failedDocs)
		docsDropped = append(docsDropped, dropped...)
		// nothing to retry
		if len(docsToSend) == 0 {
			return nil
		}

		s.logger.Info("search store retrier: send documents request failed", loglib.Fields{
			"docs_sent":     total,
			"docs_failed":   len(failedDocs),
			"docs_to_retry": len(docsToSend),
		})
		return errPartialDocumentSend
	}

	numRetries := 0
	reportErr := func(err error, d time.Duration) {
		s.logger.Error(err, "search store retrier: failed to send documents", loglib.Fields{
			"retries": numRetries,
			"backoff": d,
		})
		numRetries++
	}

	bo := s.backoffProvider(ctx)
	err := bo.RetryNotify(func() error { return send(ctx) }, reportErr)
	if err != nil {
		// some documents failed to send - return back whatever failed
		if errors.Is(err, errPartialDocumentSend) {
			return append(failedDocs, docsDropped...), nil
		}
		// internal search store error points to something wrong, return the error
		// along with the failed documents
		return append(failedDocs, docsDropped...), err
	}

	return docsDropped, nil
}

func (s *StoreRetrier) getRetriableDocs(failedDocs []DocumentError) ([]Document, []DocumentError) {
	if len(failedDocs) == 0 {
		return nil, nil
	}

	docsToRetry := make([]Document, 0, len(failedDocs))
	docsDropped := make([]DocumentError, 0, len(failedDocs))
	for _, f := range failedDocs {
		switch f.Severity {
		case SeverityDataLoss:
			docsDropped = append(docsDropped, f)
		case SeverityRetriable:
			docsToRetry = append(docsToRetry, f.Document)
		}
		if f.Severity != SeverityNone {
			s.logFailure(f)
		}
	}

	if len(docsDropped) > 0 {
		s.logger.Warn(nil, "search store retrier: documents dropped", loglib.Fields{
			"docs_failed":  len(failedDocs),
			"docs_dropped": len(docsDropped),
		})
	}

	return docsToRetry, docsDropped
}

func (s *StoreRetrier) logFailure(docErr DocumentError) {
	docBytes, err := s.marshaler(docErr.Document.Data)
	if err != nil {
		docBytes = []byte(fmt.Sprintf("failed to marshal document data: %s", err))
	}
	op := "write"
	if docErr.Document.Delete {
		op = "delete"
	}

	s.logger.Error(errors.New(docErr.Error), "search store retrier: send documents", loglib.Fields{
		"severity":  docErr.Severity.String(),
		"operation": op,
		"doc_id":    docErr.Document.ID,
		"doc":       docBytes,
		"schema":    docErr.Document.Schema,
	})
}

func (c *StoreRetryConfig) backoffConfig() *backoff.Config {
	if c != nil && (c.Backoff.Constant != nil || c.Backoff.Exponential != nil) {
		return &c.Backoff
	}
	return &backoff.Config{
		Exponential: &backoff.ExponentialConfig{
			InitialInterval: defaultStoreRetryInitialInterval,
			MaxInterval:     defaultStoreRetryMaxInterval,
			MaxRetries:      defaultStoreRetryMaxRetries,
		},
	}
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
