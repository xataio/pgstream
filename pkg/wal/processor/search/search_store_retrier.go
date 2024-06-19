// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/internal/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
)

// StoreRetrier applies a retry strategy to failed search store operations.
type StoreRetrier struct {
	inner           Store
	logger          loglib.Logger
	backoffProvider backoff.Provider
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

func NewStoreRetrier(s Store, cfg *StoreRetryConfig, opts ...StoreOption) *StoreRetrier {
	sr := &StoreRetrier{
		inner:           s,
		logger:          loglib.NewNoopLogger(),
		backoffProvider: backoff.NewProvider(cfg.backoffConfig()),
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
	return s.inner.DeleteSchema(ctx, schemaName)
}

func (s *StoreRetrier) DeleteTableDocuments(ctx context.Context, schemaName string, tableIDs []string) error {
	return s.inner.DeleteTableDocuments(ctx, schemaName, tableIDs)
}

// SendDocuments will go over failed documents, identifying any with retriable
// errors and retrying them with the configured backoff policy.
func (s *StoreRetrier) SendDocuments(ctx context.Context, docs []Document) ([]DocumentError, error) {
	docsToSend := docs
	failedDocs := []DocumentError{}
	send := func(ctx context.Context) error {
		total := len(docsToSend)
		var err error
		failedDocs, err = s.inner.SendDocuments(ctx, docsToSend)
		if err != nil {
			return err
		}

		docsToSend = s.getRetriableDocs(failedDocs)
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
			return failedDocs, nil
		}
		// internal search store error points to something wrong, return the error
		// along with the failed documents
		return failedDocs, err
	}

	return nil, nil
}

func (s *StoreRetrier) getRetriableDocs(failedDocs []DocumentError) []Document {
	if len(failedDocs) == 0 {
		return nil
	}

	dropped := 0
	docsToRetry := make([]Document, 0, len(failedDocs))
	for _, f := range failedDocs {
		switch f.Severity {
		case SeverityDataLoss:
			dropped++
		case SeverityRetriable:
			docsToRetry = append(docsToRetry, f.Document)
		}
		if f.Severity != SeverityNone {
			s.logFailure(f)
		}
	}

	if dropped > 0 {
		s.logger.Warn(nil, "search store retrier: documents dropped", loglib.Fields{
			"docs_failed":  len(failedDocs),
			"docs_dropped": dropped,
		})
	}

	return docsToRetry
}

func (s *StoreRetrier) logFailure(docErr DocumentError) {
	docBytes, err := json.Marshal(docErr.Document.Data)
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
