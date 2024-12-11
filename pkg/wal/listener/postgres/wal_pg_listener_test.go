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
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationmocks "github.com/xataio/pgstream/pkg/wal/replication/mocks"
)

func TestListener_Listen(t *testing.T) {
	t.Parallel()

	emptyMessage := &replication.Message{
		Data: nil,
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
			CommitPosition: wal.CommitPosition(testLSNStr),
		}, data)
		return nil
	}

	tests := []struct {
		name               string
		replicationHandler func(doneChan chan struct{}) *replicationmocks.Handler
		processEventFn     listenerProcessWalEvent
		generator          func(doneChan chan struct{}) snapshotGenerator
		deserialiser       func([]byte, any) error

		wantErr error
	}{
		{
			name: "ok - message received",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
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

			wantErr: context.Canceled,
		},
		{
			name: "ok - with initial snapshot",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.GetCurrentLSNFn = func(context.Context) (replication.LSN, error) {
					return testLSN, nil
				}
				h.StartReplicationFromLSNFn = func(ctx context.Context, lsn replication.LSN) error {
					require.Equal(t, testLSN, lsn)
					return nil
				}
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
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
			generator: func(_ chan struct{}) snapshotGenerator {
				return &mockGenerator{
					createSnapshotFn: func(ctx context.Context) error { return nil },
				}
			},

			wantErr: context.Canceled,
		},
		{
			name: "ok - timeout on receive message, retried",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
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

			wantErr: context.Canceled,
		},
		{
			name: "ok - nil msg data",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
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

			wantErr: context.Canceled,
		},
		{
			name: "ok - keep alive",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
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
			processEventFn: func(_ context.Context, data *wal.Event) error {
				require.Equal(t, &wal.Event{
					CommitPosition: wal.CommitPosition(testLSNStr),
				}, data)
				return nil
			},

			wantErr: context.Canceled,
		},
		{
			name: "error - starting replication",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.StartReplicationFn = func(ctx context.Context) error {
					defer func() { doneChan <- struct{}{} }()
					return errTest
				}
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
					return nil, errors.New("ReceiveMessageFn: should not be called")
				}
				return h
			},
			processEventFn: okProcessEvent,

			wantErr: errTest,
		},
		{
			name: "error - receiving message",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
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

			wantErr: errTest,
		},
		{
			name: "error - getting current LSN with initial snapshot",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.GetCurrentLSNFn = func(context.Context) (replication.LSN, error) {
					defer func() { doneChan <- struct{}{} }()
					return 0, errTest
				}
				h.StartReplicationFromLSNFn = func(ctx context.Context, lsn replication.LSN) error {
					return errors.New("StartReplicationFromLSNFn: should not be called")
				}
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
					return nil, errors.New("ReceiveMessageFn: should not be called")
				}
				return h
			},
			processEventFn: okProcessEvent,
			generator: func(doneChan chan struct{}) snapshotGenerator {
				return &mockGenerator{
					createSnapshotFn: func(ctx context.Context) error { return errors.New("createSnapshotFn: should not be called") },
				}
			},

			wantErr: errTest,
		},
		{
			name: "error - creating initial snapshot",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.GetCurrentLSNFn = func(context.Context) (replication.LSN, error) {
					return testLSN, nil
				}
				h.StartReplicationFromLSNFn = func(ctx context.Context, lsn replication.LSN) error {
					return errors.New("StartReplicationFromLSNFn: should not be called")
				}
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
					return nil, errors.New("ReceiveMessageFn: should not be called")
				}
				return h
			},
			processEventFn: okProcessEvent,
			generator: func(doneChan chan struct{}) snapshotGenerator {
				return &mockGenerator{
					createSnapshotFn: func(ctx context.Context) error {
						defer func() { doneChan <- struct{}{} }()
						return errTest
					},
				}
			},

			wantErr: errTest,
		},
		{
			name: "error - starting replication from LSN after initial snapshot",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.GetCurrentLSNFn = func(context.Context) (replication.LSN, error) {
					return testLSN, nil
				}
				h.StartReplicationFromLSNFn = func(ctx context.Context, lsn replication.LSN) error {
					defer func() { doneChan <- struct{}{} }()
					return errTest
				}
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
					return nil, errors.New("ReceiveMessageFn: should not be called")
				}
				return h
			},
			processEventFn: okProcessEvent,
			generator: func(doneChan chan struct{}) snapshotGenerator {
				return &mockGenerator{createSnapshotFn: func(ctx context.Context) error { return nil }}
			},

			wantErr: errTest,
		},
		{
			name: "error - processing wal event",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
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

			wantErr: errTest,
		},
		{
			name: "error - deserialising wal event",
			replicationHandler: func(doneChan chan struct{}) *replicationmocks.Handler {
				h := newMockReplicationHandler()
				h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
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
			deserialiser:   func(b []byte, a any) error { return errTest },

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			opts := []Option{
				WithLogger(log.NewNoopLogger()),
			}
			if tc.generator != nil {
				opts = append(opts, WithInitialSnapshot(tc.generator(doneChan)))
			}

			replicationHandler := tc.replicationHandler(doneChan)
			l := New(replicationHandler, tc.processEventFn, opts...)
			l.walDataDeserialiser = testDeserialiser
			l.lsnParser = newMockLSNParser()
			defer l.Close()

			if tc.deserialiser != nil {
				l.walDataDeserialiser = tc.deserialiser
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := l.Listen(ctx)
				require.ErrorIs(t, err, tc.wantErr)
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
