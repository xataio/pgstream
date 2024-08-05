// SPDX-License-Identifier: Apache-2.0

package search

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/backoff/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
)

func TestSchemaCleaner_deleteSchema(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"

	tests := []struct {
		name      string
		queueSize uint

		wantErr error
	}{
		{
			name:      "ok",
			queueSize: 10,

			wantErr: nil,
		},
		{
			name:      "error - registration timeout",
			queueSize: 0,

			wantErr: errRegistrationTimeout,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			schemaCleaner := &schemaCleaner{
				logger:              loglib.NewNoopLogger(),
				registrationTimeout: time.Second,
				deleteSchemaQueue:   make(chan string, tc.queueSize),
			}
			defer schemaCleaner.stop()

			err := schemaCleaner.deleteSchema(context.Background(), testSchemaName)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestSchemaCleaner_start(t *testing.T) {
	t.Parallel()

	testSchemaName := "test_schema"
	errTest := errors.New("oh noes")

	tests := []struct {
		name            string
		store           store
		backoffProvider func(doneChan chan struct{}) backoff.Provider
	}{
		{
			name: "ok",
			store: &mockStore{
				deleteSchemaFn: func(ctx context.Context, schemaName string) error {
					require.Equal(t, testSchemaName, schemaName)
					return nil
				},
			},
			backoffProvider: func(doneChan chan struct{}) backoff.Provider {
				once := sync.Once{}
				return func(ctx context.Context) backoff.Backoff {
					return &mocks.Backoff{
						RetryNotifyFn: func(o backoff.Operation, n backoff.Notify) error {
							defer once.Do(func() { doneChan <- struct{}{} })
							return o()
						},
					}
				}
			},
		},
		{
			name: "error deleting schema",
			store: &mockStore{
				deleteSchemaFn: func(ctx context.Context, schemaName string) error {
					return errTest
				},
			},
			backoffProvider: func(doneChan chan struct{}) backoff.Provider {
				once := sync.Once{}
				return func(ctx context.Context) backoff.Backoff {
					return &mocks.Backoff{
						RetryNotifyFn: func(o backoff.Operation, n backoff.Notify) error {
							defer once.Do(func() { doneChan <- struct{}{} })
							err := o()
							if err != nil {
								n(err, 50*time.Millisecond)
							}
							return err
						},
					}
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			schemaCleaner := &schemaCleaner{
				logger:              loglib.NewNoopLogger(),
				store:               tc.store,
				backoffProvider:     tc.backoffProvider(doneChan),
				registrationTimeout: defaultRegistrationTimeout,
				deleteSchemaQueue:   make(chan string, 100),
			}
			defer schemaCleaner.stop()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				schemaCleaner.start(ctx)
			}()

			schemaCleaner.deleteSchemaQueue <- testSchemaName

			for {
				select {
				case <-ctx.Done():
					t.Errorf("test timeout reached")
					wg.Wait()
					return
				case <-doneChan:
					cancel()
					wg.Wait()
					return
				}
			}
		})
	}
}
