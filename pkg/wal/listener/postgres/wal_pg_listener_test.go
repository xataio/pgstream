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
	"github.com/xataio/pgstream/internal/phase"
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
			name: "ok - pgstream internal dml becomes keep alive",
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
			processEventFn: func(_ context.Context, data *wal.Event) error {
				require.Equal(t, &wal.Event{
					CommitPosition: wal.CommitPosition(testLSNStr),
				}, data)
				return nil
			},
			deserialiser: func(_ []byte, out any) error {
				data, ok := out.(*wal.Data)
				if !ok {
					return fmt.Errorf("unexpected wal data type: %T", out)
				}
				*data = wal.Data{
					Action: "I",
					Schema: pgstreamSchemaName,
					Table:  "table_ids",
				}
				return nil
			},

			wantErr: context.Canceled,
		},
		{
			name: "ok - pgstream schema ddl is processed",
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
			processEventFn: func(_ context.Context, data *wal.Event) error {
				require.Equal(t, &wal.Event{
					Data: &wal.Data{
						Action: wal.LogicalMessageAction,
						Schema: pgstreamSchemaName,
						Table:  "schema_log",
						Prefix: wal.DDLPrefix,
					},
					CommitPosition: wal.CommitPosition(testLSNStr),
				}, data)
				return nil
			},
			deserialiser: func(_ []byte, out any) error {
				data, ok := out.(*wal.Data)
				if !ok {
					return fmt.Errorf("unexpected wal data type: %T", out)
				}
				*data = wal.Data{
					Action: wal.LogicalMessageAction,
					Schema: pgstreamSchemaName,
					Table:  "schema_log",
					Prefix: wal.DDLPrefix,
				}
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

func TestListener_Listen_PhaseTransitions(t *testing.T) {
	t.Parallel()

	t.Run("replication only sets replication after start", func(t *testing.T) {
		t.Parallel()

		tracker := phase.NewTracker()
		doneChan := make(chan struct{}, 1)

		h := newMockReplicationHandler()
		h.StartReplicationFn = func(ctx context.Context) error {
			require.Equal(t, phase.Phase(""), tracker.Get())
			return nil
		}
		h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
			require.Equal(t, phase.Replication, tracker.Get())
			doneChan <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}

		l := New(h, func(context.Context, *wal.Event) error { return nil },
			WithLogger(log.NewNoopLogger()),
			WithPhaseTracker(tracker))
		l.lsnParser = newMockLSNParser()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() { errCh <- l.Listen(ctx) }()

		select {
		case <-doneChan:
			cancel()
		case <-ctx.Done():
			t.Fatal("timeout waiting for receive")
		}
		require.ErrorIs(t, <-errCh, context.Canceled)
		require.Equal(t, phase.Replication, tracker.Get())
	})

	t.Run("snapshot and replication flips at StartReplicationFromLSN", func(t *testing.T) {
		t.Parallel()

		tracker := phase.NewTracker()
		doneChan := make(chan struct{}, 1)

		h := newMockReplicationHandler()
		h.GetCurrentLSNFn = func(context.Context) (replication.LSN, error) {
			require.Equal(t, phase.Snapshot, tracker.Get())
			return testLSN, nil
		}
		h.StartReplicationFromLSNFn = func(ctx context.Context, lsn replication.LSN) error {
			require.Equal(t, phase.Snapshot, tracker.Get())
			require.Equal(t, testLSN, lsn)
			return nil
		}
		h.ReceiveMessageFn = func(ctx context.Context, i uint64) (*replication.Message, error) {
			require.Equal(t, phase.Replication, tracker.Get())
			doneChan <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}

		gen := &mockGenerator{
			createSnapshotFn: func(ctx context.Context) error {
				require.Equal(t, phase.Snapshot, tracker.Get())
				return nil
			},
		}

		l := New(h, func(context.Context, *wal.Event) error { return nil },
			WithLogger(log.NewNoopLogger()),
			WithInitialSnapshot(gen),
			WithPhaseTracker(tracker))
		l.lsnParser = newMockLSNParser()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() { errCh <- l.Listen(ctx) }()

		select {
		case <-doneChan:
			cancel()
		case <-ctx.Done():
			t.Fatal("timeout waiting for receive")
		}
		require.ErrorIs(t, <-errCh, context.Canceled)
		require.Equal(t, phase.Replication, tracker.Get())
	})
}

func TestListener_ProcessWALEvent_TransactionMetadata(t *testing.T) {
	var events []*wal.Event
	l := New(newMockReplicationHandler(), func(_ context.Context, event *wal.Event) error {
		events = append(events, event)
		return nil
	})
	l.lsnParser = &replicationmocks.LSNParser{
		ToStringFn: func(lsn replication.LSN) string { return fmt.Sprintf("%d", lsn) },
		FromStringFn: func(string) (replication.LSN, error) {
			return 0, nil
		},
	}
	l.walDataDeserialiser = func(data []byte, out any) error {
		walData, ok := out.(*wal.Data)
		if !ok {
			return fmt.Errorf("unexpected wal data type: %T", out)
		}
		walData.Action = string(data)
		return nil
	}

	for i, action := range []string{"B", "I", "C", "B", "U", "C"} {
		err := l.processWALEvent(context.Background(), &replication.Message{
			LSN:  replication.LSN(i + 10),
			Data: []byte(action),
		})
		require.NoError(t, err)
	}

	require.Len(t, events, 6)
	require.Nil(t, events[0].Data)
	require.Equal(t, &wal.Transaction{ID: 1, Boundary: wal.TransactionBegin}, events[0].Transaction)
	require.Equal(t, wal.CommitPosition("10"), events[0].CommitPosition)

	require.Equal(t, "I", events[1].Data.Action)
	require.Equal(t, &wal.Transaction{ID: 1, Boundary: wal.TransactionEntry}, events[1].Transaction)

	require.Nil(t, events[2].Data)
	require.Equal(t, &wal.Transaction{ID: 1, Boundary: wal.TransactionCommit}, events[2].Transaction)
	require.Equal(t, wal.CommitPosition("12"), events[2].CommitPosition)

	require.Equal(t, &wal.Transaction{ID: 2, Boundary: wal.TransactionBegin}, events[3].Transaction)
	require.Equal(t, &wal.Transaction{ID: 2, Boundary: wal.TransactionEntry}, events[4].Transaction)
	require.Equal(t, &wal.Transaction{ID: 2, Boundary: wal.TransactionCommit}, events[5].Transaction)
}

func TestListener_ProcessWALEvent_ResetsTransactionStateForNewGeneration(t *testing.T) {
	var events []*wal.Event
	l := New(newMockReplicationHandler(), func(_ context.Context, event *wal.Event) error {
		events = append(events, event)
		return nil
	})
	l.lsnParser = &replicationmocks.LSNParser{
		ToStringFn:   func(lsn replication.LSN) string { return fmt.Sprintf("%d", lsn) },
		FromStringFn: func(string) (replication.LSN, error) { return 0, nil },
	}
	l.walDataDeserialiser = func(data []byte, out any) error {
		walData, ok := out.(*wal.Data)
		if !ok {
			return fmt.Errorf("unexpected wal data type: %T", out)
		}
		walData.Action = string(data)
		return nil
	}

	// Generation zero is interrupted before its commit record arrives.
	require.NoError(t, l.processWALEvent(context.Background(), &replication.Message{
		LSN: 10, Data: []byte("B"), Generation: 0,
	}))
	require.NoError(t, l.processWALEvent(context.Background(), &replication.Message{
		LSN: 11, Data: []byte("I"), Generation: 0,
	}))

	// The replayed B record starts a fresh local transaction instead of
	// conflicting with the transaction left open by the previous connection.
	require.NoError(t, l.processWALEvent(context.Background(), &replication.Message{
		LSN: 10, Data: []byte("B"), Generation: 1,
	}))
	require.NoError(t, l.processWALEvent(context.Background(), &replication.Message{
		LSN: 11, Data: []byte("I"), Generation: 1,
	}))
	require.NoError(t, l.processWALEvent(context.Background(), &replication.Message{
		LSN: 12, Data: []byte("C"), Generation: 1,
	}))

	require.Len(t, events, 5)
	require.Equal(t, uint64(0), events[0].Generation)
	require.Equal(t, &wal.Transaction{ID: 1, Boundary: wal.TransactionBegin}, events[0].Transaction)
	require.Equal(t, uint64(1), events[2].Generation)
	require.Equal(t, &wal.Transaction{ID: 1, Boundary: wal.TransactionBegin}, events[2].Transaction)
	require.Equal(t, &wal.Transaction{ID: 1, Boundary: wal.TransactionEntry}, events[3].Transaction)
	require.Equal(t, &wal.Transaction{ID: 1, Boundary: wal.TransactionCommit}, events[4].Transaction)
}

func TestListener_ProcessWALEvent_StampsKeepaliveGeneration(t *testing.T) {
	var event *wal.Event
	l := New(newMockReplicationHandler(), func(_ context.Context, received *wal.Event) error {
		event = received
		return nil
	})
	l.lsnParser = newMockLSNParser()

	require.NoError(t, l.processWALEvent(context.Background(), &replication.Message{
		LSN:            testLSN,
		ReplyRequested: true,
		Generation:     3,
	}))
	require.NotNil(t, event)
	require.Equal(t, uint64(3), event.Generation)
	require.Nil(t, event.Data)
}
