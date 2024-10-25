// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
)

func TestStoreRetrier_DeleteSchema(t *testing.T) {
	t.Parallel()

	const testSchema = "test-schema"
	tests := []struct {
		name  string
		store *mockStore

		wantErr error
	}{
		{
			name: "ok",
			store: &mockStore{
				deleteSchemaFn: func(ctx context.Context, _ uint, schemaName string) error {
					require.Equal(t, testSchema, schemaName)
					return nil
				},
			},
			wantErr: nil,
		},
		{
			name: "ok - retriable error",
			store: &mockStore{
				deleteSchemaFn: func(ctx context.Context, i uint, schemaName string) error {
					require.Equal(t, testSchema, schemaName)
					switch i {
					case 1:
						return ErrRetriable
					case 2:
						return nil
					default:
						return fmt.Errorf("unexpected call to deleteSchema: %d", i)
					}
				},
			},
			wantErr: nil,
		},
		{
			name: "err - retriable error backoff exhausted",
			store: &mockStore{
				deleteSchemaFn: func(ctx context.Context, i uint, schemaName string) error {
					require.Equal(t, testSchema, schemaName)
					switch i {
					case 1, 2, 3:
						return ErrRetriable
					default:
						return fmt.Errorf("unexpected call to deleteSchema: %d", i)
					}
				},
			},
			wantErr: ErrRetriable,
		},
		{
			name: "err - permanent error",
			store: &mockStore{
				deleteSchemaFn: func(ctx context.Context, i uint, schemaName string) error {
					require.Equal(t, testSchema, schemaName)
					switch i {
					case 1:
						return errTest
					default:
						return fmt.Errorf("unexpected call to deleteSchema: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			retrier := StoreRetrier{
				inner:           tc.store,
				logger:          loglib.NewNoopLogger(),
				backoffProvider: newMockBackoffProvider(),
			}
			err := retrier.DeleteSchema(context.Background(), testSchema)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStoreRetrier_SendDocuments(t *testing.T) {
	t.Parallel()

	testDocs := []Document{
		*newTestDocument(withID("1")),
		*newTestDocument(withID("2")),
		*newTestDocument(withID("3")),
	}

	failedDocs := func(severity Severity) []DocumentError {
		return []DocumentError{
			{
				Document: *newTestDocument(withID("1")),
				Severity: severity,
				Error:    errTest.Error(),
			},
		}
	}

	tests := []struct {
		name            string
		store           *mockStore
		backoffProvider backoff.Provider

		wantFailedDocs []DocumentError
		wantErr        error
	}{
		{
			name: "ok",
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					require.Equal(t, testDocs, docs)
					return nil, nil
				},
			},
			wantFailedDocs: []DocumentError{},
			wantErr:        nil,
		},
		{
			name: "ok - transient error",
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					switch i {
					case 1:
						require.Equal(t, testDocs, docs)
						return nil, errTest
					case 2:
						require.Equal(t, testDocs, docs)
						return nil, nil
					default:
						return nil, fmt.Errorf("sendDocumentsFn: unexpected call %d", i)
					}
				},
			},
			wantFailedDocs: []DocumentError{},
			wantErr:        nil,
		},
		{
			name: "ok - failed and dropped documents",
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					switch i {
					case 1:
						require.Equal(t, testDocs, docs)
						return append(failedDocs(SeverityDataLoss), failedDocs(SeverityRetriable)...), nil
					case 2, 3:
						require.Equal(t, []Document{*newTestDocument(withID("1"))}, docs)
						return failedDocs(SeverityRetriable), nil
					default:
						return nil, fmt.Errorf("sendDocumentsFn: unexpected call %d", i)
					}
				},
			},
			wantFailedDocs: append(failedDocs(SeverityRetriable), failedDocs(SeverityDataLoss)...),
			wantErr:        nil,
		},
		{
			name: "ok - all failed documents dropped",
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					switch i {
					case 1:
						require.Equal(t, testDocs, docs)
						return failedDocs(SeverityDataLoss), nil
					default:
						return nil, fmt.Errorf("sendDocumentsFn: unexpected call %d", i)
					}
				},
			},
			wantFailedDocs: failedDocs(SeverityDataLoss),
			wantErr:        nil,
		},
		{
			name: "ok - some failed documents",
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					switch i {
					case 1:
						require.Equal(t, testDocs, docs)
						return failedDocs(SeverityRetriable), nil
					case 2, 3:
						require.Equal(t, []Document{*newTestDocument(withID("1"))}, docs)
						return failedDocs(SeverityRetriable), nil
					default:
						return nil, fmt.Errorf("sendDocumentsFn: unexpected call %d", i)
					}
				},
			},
			wantFailedDocs: failedDocs(SeverityRetriable),
			wantErr:        nil,
		},
		{
			name: "error - store error",
			store: &mockStore{
				sendDocumentsFn: func(ctx context.Context, i uint, docs []Document) ([]DocumentError, error) {
					return nil, errTest
				},
			},
			wantFailedDocs: nil,
			wantErr:        errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			retrier := StoreRetrier{
				inner:           tc.store,
				logger:          loglib.NewNoopLogger(),
				backoffProvider: newMockBackoffProvider(),
				marshaler:       json.Marshal,
			}

			failedDocs, err := retrier.SendDocuments(context.Background(), testDocs)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantFailedDocs, failedDocs)
		})
	}
}

// mock backoff provider runs the operation for up to 2 times until it succeeds
// or returns error
func newMockBackoffProvider() backoff.Provider {
	return func(ctx context.Context) backoff.Backoff {
		return backoff.NewConstantBackoff(ctx, &backoff.ConstantConfig{
			Interval:   0,
			MaxRetries: 2,
		})
	}
}
