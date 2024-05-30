// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/replication"
	replicationmocks "github.com/xataio/pgstream/internal/replication/mocks"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestListener_Listen(t *testing.T) {
	t.Parallel()

	emptyMessage := &replicationmocks.Message{
		GetDataFn: func() *replication.MessageData {
			return nil
		},
	}

	testDeserialiser := func(_ []byte, out any) error {
		data, ok := out.(*wal.Data)
		if !ok {
			return fmt.Errorf("unexpected wal data type: %T", out)
		}
		*data = wal.Data{
			Action: "I",
		}
		return nil
	}

	errTest := errors.New("oh noes")
	okProcessEvent := func(_ context.Context, data *wal.Event) error {
		require.Equal(t, &wal.Event{
			Data: &wal.Data{
				Action: "I",
			},
			CommitPosition: wal.CommitPosition{
				PGPos: testLSN,
			},
		}, data)
		return nil
	}

	tests := []struct {
		name               string
		replicationHandler func(doneChan chan struct{}) *replicationmocks.Handler
		processEventFn     listenerProcessWalEvent
		syncInterval       time.Duration

		wantSyncCalls uint64
		wantErr       error
	}{
		{
			name: "ok - message received",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 1 {
							doneChan <- struct{}{}
						}
					}()
					switch i {
					case 1:
						return newMockMessage(), nil
					default:
						return emptyMessage, nil
					}
				}
				return h
			},
			processEventFn: okProcessEvent,

			wantSyncCalls: 1,
			wantErr:       nil,
		},
		{
			name: "ok - sync interval",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					return emptyMessage, nil
				}
				h.SyncLSNFn = func(ctx context.Context) error {
					defer func() {
						if i := h.GetSyncLSNCalls(); i == 1 {
							doneChan <- struct{}{}
						}
					}()
					return nil
				}
				return h
			},
			processEventFn: okProcessEvent,
			syncInterval:   50 * time.Millisecond,

			wantSyncCalls: 2,
			wantErr:       nil,
		},
		{
			name: "ok - timeout on receive message, retried",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 2 {
							doneChan <- struct{}{}
						}
					}()
					switch i {
					case 1:
						return nil, replication.ErrConnTimeout
					case 2:
						return newMockMessage(), nil
					default:
						return emptyMessage, nil
					}
				}
				return h
			},
			processEventFn: okProcessEvent,

			wantSyncCalls: 1,
			wantErr:       nil,
		},
		{
			name: "ok - nil msg data",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 1 {
							doneChan <- struct{}{}
						}
					}()
					return emptyMessage, nil
				}
				return h
			},
			processEventFn: okProcessEvent,

			wantSyncCalls: 1,
			wantErr:       nil,
		},
		{
			name: "ok - keep alive no reply requested",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.UpdateLSNPositionFn = func(lsn replication.LSN) {
					require.Equal(t, testLSN, lsn)
				}
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 1 {
							doneChan <- struct{}{}
						}
					}()
					switch i {
					case 1:
						return newMockKeepAliveMessage(false), nil
					default:
						return emptyMessage, nil
					}
				}
				return h
			},
			processEventFn: okProcessEvent,

			wantSyncCalls: 1,
			wantErr:       nil,
		},
		{
			name: "ok - keep alive with reply requested",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.UpdateLSNPositionFn = func(lsn replication.LSN) {
					require.Equal(t, testLSN, lsn)
				}
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 1 {
							doneChan <- struct{}{}
						}
					}()
					switch i {
					case 1:
						return newMockKeepAliveMessage(true), nil
					default:
						return emptyMessage, nil
					}
				}
				return h
			},
			processEventFn: okProcessEvent,

			wantSyncCalls: 2,
			wantErr:       nil,
		},
		{
			name: "error - keep alive",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.UpdateLSNPositionFn = func(lsn replication.LSN) {
					require.Equal(t, testLSN, lsn)
				}
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 1 {
							doneChan <- struct{}{}
						}
					}()
					switch i {
					case 1:
						return newMockKeepAliveMessage(true), nil
					default:
						return emptyMessage, nil
					}
				}
				h.SyncLSNFn = func(context.Context) error { return errTest }

				return h
			},
			processEventFn: okProcessEvent,

			wantSyncCalls: 1,
			wantErr:       errTest,
		},
		{
			name: "error - receiving message",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 1 {
							doneChan <- struct{}{}
						}
					}()
					return nil, errTest
				}
				return h
			},
			processEventFn: okProcessEvent,

			wantSyncCalls: 0,
			wantErr:       errTest,
		},
		{
			name: "error - processing wal event",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 1 {
							doneChan <- struct{}{}
						}
					}()
					return newMockMessage(), nil
				}
				return h
			},
			processEventFn: func(context.Context, *wal.Event) error { return errTest },

			wantSyncCalls: 0,
			wantErr:       errTest,
		},
		{
			name: "error - context canceled during process wal event",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					defer func() {
						if i == 1 {
							doneChan <- struct{}{}
						}
					}()
					return newMockMessage(), nil
				}
				return h
			},
			processEventFn: func(context.Context, *wal.Event) error { return nil },

			wantSyncCalls: 1,
			wantErr:       nil,
		},
		{
			name: "error - sync interval",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (replication.Message, error) {
					return emptyMessage, nil
				}
				h.SyncLSNFn = func(ctx context.Context) error {
					defer func() {
						if i := h.GetSyncLSNCalls(); i == 1 {
							doneChan <- struct{}{}
						}
					}()
					return errTest
				}
				return h
			},
			processEventFn: okProcessEvent,
			syncInterval:   50 * time.Millisecond,

			wantSyncCalls: 1,
			wantErr:       errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			syncInterval := 5 * time.Second
			if tc.syncInterval != 0 {
				syncInterval = tc.syncInterval
			}

			replicationHandler := tc.replicationHandler(doneChan)
			l := &Listener{
				replicationHandler:  replicationHandler,
				processEvent:        tc.processEventFn,
				syncInterval:        syncInterval,
				walDataDeserialiser: testDeserialiser,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := l.Listen(ctx)
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, int(tc.wantSyncCalls), int(replicationHandler.GetSyncLSNCalls()))
			}()

			// make sure the test doesn't block indefinitely if something goes
			// wrong
			for {
				select {
				case <-doneChan:
					cancel()
					wg.Wait()
					return
				case <-ctx.Done():
					t.Log("test timeout waiting for listen")
					wg.Wait()
					return
				}
			}
		})
	}
}
