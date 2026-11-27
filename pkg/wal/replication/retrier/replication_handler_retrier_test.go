// SPDX-License-Identifier: Apache-2.0

package retrier

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/replication"
	"github.com/xataio/pgstream/pkg/wal/replication/mocks"
)

func TestHandlerRetrier_ReceiveMessage(t *testing.T) {
	t.Parallel()

	retriableErr := errors.New("retriable error")
	nonRetriableErr := context.Canceled
	resetConnErr := errors.New("reset connection error")

	tests := []struct {
		name       string
		handler    replication.Handler
		boProvider backoff.Provider

		wantMsg *replication.Message
		wantErr error
	}{
		{
			name: "ok",
			handler: &mocks.Handler{
				ReceiveMessageFn: func(_ context.Context, _ uint64) (*replication.Message, error) {
					return &replication.Message{
						Data: []byte("test message"),
					}, nil
				},
			},
			boProvider: newMockBackoffProvider(),

			wantMsg: &replication.Message{
				Data: []byte("test message"),
			},
			wantErr: nil,
		},
		{
			name: "retriable error then success",
			handler: &mocks.Handler{
				ReceiveMessageFn: func(_ context.Context, i uint64) (*replication.Message, error) {
					switch i {
					case 1, 2:
						return nil, retriableErr
					case 3:
						return &replication.Message{
							Data: []byte("test message"),
						}, nil
					default:
						return nil, fmt.Errorf("unexpected call to ReceiveMessage: %w", nonRetriableErr)
					}
				},
				ResetConnectionFn: func(ctx context.Context) error {
					return nil
				},
			},
			boProvider: newMockBackoffProvider(),

			wantMsg: &replication.Message{
				Data:       []byte("test message"),
				Generation: 1,
			},
			wantErr: nil,
		},
		{
			name: "retriable error then permanent error",
			handler: &mocks.Handler{
				ReceiveMessageFn: func(_ context.Context, i uint64) (*replication.Message, error) {
					require.Contains(t, []uint64{1, 2, 3}, i)
					switch i {
					case 1, 2:
						return nil, retriableErr
					default:
						return nil, nonRetriableErr
					}
				},
				ResetConnectionFn: func(ctx context.Context) error {
					return nil
				},
			},
			boProvider: newMockBackoffProvider(),

			wantMsg: nil,
			wantErr: nonRetriableErr,
		},
		{
			name: "retriable error with error resetting connection",
			handler: &mocks.Handler{
				ReceiveMessageFn: func(_ context.Context, i uint64) (*replication.Message, error) {
					return nil, retriableErr
				},
				ResetConnectionFn: func(ctx context.Context) error {
					return resetConnErr
				},
			},
			boProvider: newMockBackoffProvider(),

			wantMsg: nil,
			wantErr: resetConnErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hr := HandlerRetrier{
				inner:           tc.handler,
				backoffProvider: tc.boProvider,
				logger:          log.NewNoopLogger(),
			}

			msg, err := hr.ReceiveMessage(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantMsg, msg)
		})
	}
}

func TestHandlerRetrier_ReceiveMessageDiscardsReceiveOverlappingReset(t *testing.T) {
	receiveStarted := make(chan struct{})
	releaseOldReceive := make(chan struct{})
	handler := &mocks.Handler{
		ReceiveMessageFn: func(_ context.Context, call uint64) (*replication.Message, error) {
			switch call {
			case 1:
				close(receiveStarted)
				<-releaseOldReceive
				return &replication.Message{Data: []byte("old connection")}, nil
			case 2:
				return &replication.Message{Data: []byte("new connection")}, nil
			default:
				return nil, fmt.Errorf("unexpected receive call: %d", call)
			}
		},
		ResetConnectionFn: func(context.Context) error { return nil },
	}
	retrier := HandlerRetrier{
		inner:           handler,
		backoffProvider: newMockBackoffProvider(),
		logger:          log.NewNoopLogger(),
	}

	type result struct {
		message *replication.Message
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		message, err := retrier.ReceiveMessage(context.Background())
		resultCh <- result{message: message, err: err}
	}()

	<-receiveStarted
	require.NoError(t, retrier.ResetConnection(context.Background()))
	close(releaseOldReceive)

	got := <-resultCh
	require.NoError(t, got.err)
	require.Equal(t, &replication.Message{
		Data:       []byte("new connection"),
		Generation: 1,
	}, got.message)
}

func TestHandlerRetrier_SyncLSN(t *testing.T) {
	t.Parallel()

	retriableErr := errors.New("retriable error")
	nonRetriableErr := context.Canceled
	resetConnErr := errors.New("reset connection error")

	testLSN := replication.LSN(12345)

	tests := []struct {
		name       string
		handler    replication.Handler
		boProvider backoff.Provider
		lsn        replication.LSN

		wantErr error
	}{
		{
			name: "ok",
			handler: &mocks.Handler{
				SyncLSNFn: func(_ context.Context, _ replication.LSN, _ uint64) error {
					return nil
				},
			},
			boProvider: newMockBackoffProvider(),
			lsn:        testLSN,

			wantErr: nil,
		},
		{
			name: "retriable error then success",
			handler: &mocks.Handler{
				SyncLSNFn: func(_ context.Context, lsn replication.LSN, i uint64) error {
					switch i {
					case 1, 2:
						return retriableErr
					case 3:
						return nil
					default:
						return fmt.Errorf("unexpected call to SyncLSN: %w", nonRetriableErr)
					}
				},
				ResetConnectionFn: func(ctx context.Context) error {
					return nil
				},
			},
			boProvider: newMockBackoffProvider(),
			lsn:        testLSN,

			wantErr: nil,
		},
		{
			name: "retriable error then permanent error",
			handler: &mocks.Handler{
				SyncLSNFn: func(_ context.Context, lsn replication.LSN, i uint64) error {
					require.Contains(t, []uint64{1, 2, 3}, i)
					switch i {
					case 1, 2:
						return retriableErr
					default:
						return nonRetriableErr
					}
				},
				ResetConnectionFn: func(ctx context.Context) error {
					return nil
				},
			},
			boProvider: newMockBackoffProvider(),
			lsn:        testLSN,

			wantErr: nonRetriableErr,
		},
		{
			name: "retriable error with error resetting connection",
			handler: &mocks.Handler{
				SyncLSNFn: func(_ context.Context, lsn replication.LSN, i uint64) error {
					return retriableErr
				},
				ResetConnectionFn: func(ctx context.Context) error {
					return resetConnErr
				},
			},
			boProvider: newMockBackoffProvider(),
			lsn:        testLSN,

			wantErr: resetConnErr,
		},
		{
			name: "non-retriable error no retry",
			handler: &mocks.Handler{
				SyncLSNFn: func(_ context.Context, lsn replication.LSN, i uint64) error {
					require.Equal(t, uint64(1), i)
					return nonRetriableErr
				},
			},
			boProvider: newMockBackoffProvider(),
			lsn:        testLSN,

			wantErr: nonRetriableErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hr := HandlerRetrier{
				inner:           tc.handler,
				backoffProvider: tc.boProvider,
				logger:          log.NewNoopLogger(),
			}

			err := hr.SyncLSN(context.Background(), tc.lsn)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestHandlerRetrier_SyncResetAdvancesReceiveGeneration(t *testing.T) {
	retriableErr := errors.New("retriable error")
	handler := &mocks.Handler{
		SyncLSNFn: func(_ context.Context, _ replication.LSN, call uint64) error {
			if call <= 2 {
				return retriableErr
			}
			return nil
		},
		ResetConnectionFn: func(context.Context) error { return nil },
		ReceiveMessageFn: func(context.Context, uint64) (*replication.Message, error) {
			return &replication.Message{Data: []byte("after reset")}, nil
		},
	}
	retrier := HandlerRetrier{
		inner:           handler,
		backoffProvider: newMockBackoffProvider(),
		logger:          log.NewNoopLogger(),
	}

	require.NoError(t, retrier.SyncLSN(context.Background(), 42))
	message, err := retrier.ReceiveMessage(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), message.Generation)
}

// mock backoff provider runs the operation for up to 3 times until it succeeds
// or returns error
func newMockBackoffProvider() backoff.Provider {
	return func(ctx context.Context) backoff.Backoff {
		return backoff.NewConstantBackoff(ctx, &backoff.ConstantConfig{
			Interval:   0,
			MaxRetries: 3,
		})
	}
}
