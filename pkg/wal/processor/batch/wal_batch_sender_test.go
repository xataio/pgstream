// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	syncmocks "github.com/xataio/pgstream/internal/sync/mocks"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestSender_AddToBatch(t *testing.T) {
	t.Parallel()

	noopSendFn := func(ctx context.Context, b *Batch[*mockMessage]) error { return nil }
	testCommitPos := wal.CommitPosition("1")

	testWALMsg := func(i uint) *WALMessage[*mockMessage] {
		return NewWALMessage(&mockMessage{id: i}, testCommitPos)
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name              string
		weightedSemaphore *syncmocks.WeightedSemaphore
		msg               *WALMessage[*mockMessage]
		sendDone          bool

		wantMsgs []*WALMessage[*mockMessage]
		wantErr  error
	}{
		{
			name: "ok",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool {
					require.Equal(t, int64(1), i)
					return true
				},
			},
			msg: testWALMsg(1),

			wantMsgs: []*WALMessage[*mockMessage]{
				testWALMsg(1),
			},
			wantErr: nil,
		},
		{
			name:              "ok - nil wal message",
			weightedSemaphore: &syncmocks.WeightedSemaphore{},
			msg:               nil,

			wantMsgs: []*WALMessage[*mockMessage]{},
			wantErr:  nil,
		},
		{
			name: "ok - keep alive",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool {
					require.Equal(t, int64(1), i)
					return true
				},
			},
			msg: NewWALMessage(&mockMessage{}, testCommitPos),

			wantMsgs: []*WALMessage[*mockMessage]{
				NewWALMessage(&mockMessage{}, testCommitPos),
			},
			wantErr: nil,
		},
		{
			name: "ok - waiting for semaphore",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return false },
				AcquireFn:    func(ctx context.Context, i int64) error { return nil },
			},
			msg: testWALMsg(1),

			wantMsgs: []*WALMessage[*mockMessage]{
				testWALMsg(1),
			},
			wantErr: nil,
		},
		{
			name: "error - acquiring semaphore",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return false },
				AcquireFn:    func(ctx context.Context, i int64) error { return errTest },
			},
			msg: testWALMsg(1),

			wantMsgs: []*WALMessage[*mockMessage]{},
			wantErr:  errTest,
		},
		{
			name: "error - send done",
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool {
					require.Equal(t, int64(1), i)
					return true
				},
			},
			msg:      testWALMsg(1),
			sendDone: true,

			wantErr: errSendStopped,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			batchSender, err := NewSender(&Config{}, noopSendFn, log.NewNoopLogger())
			require.NoError(t, err)
			batchSender.queueBytesSema = tc.weightedSemaphore
			defer batchSender.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if tc.sendDone {
				batchSender.sendDone <- errSendStopped
				close(batchSender.sendDone)
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer func() {
					batchSender.closeMsgChan()
					wg.Done()
				}()
				err = batchSender.AddToBatch(ctx, tc.msg)
				require.ErrorIs(t, err, tc.wantErr)
			}()

			if tc.sendDone {
				wg.Wait()
				return
			}

			msgs := []*WALMessage[*mockMessage]{}
			for msg := range batchSender.msgChan {
				if ctx.Err() != nil {
					t.Log("test timeout reached")
					return
				}
				msgs = append(msgs, msg)
			}
			require.Equal(t, tc.wantMsgs, msgs)
		})
	}
}

func TestSender_Send(t *testing.T) {
	t.Parallel()

	testCommitPos := wal.CommitPosition("1")

	mockMsg := func(i uint) *mockMessage {
		return &mockMessage{id: i}
	}
	testWALMsg := func(i uint) *WALMessage[*mockMessage] {
		return NewWALMessage(mockMsg(i), testCommitPos)
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name      string
		msgs      []*WALMessage[*mockMessage]
		semaphore *syncmocks.WeightedSemaphore
		sendFn    func(doneChan chan<- struct{}) sendBatchFn[*mockMessage]

		wantErr error
	}{
		{
			name: "ok",
			msgs: []*WALMessage[*mockMessage]{
				testWALMsg(1),
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(1), bytes)
					}
				},
			},
			sendFn: func(doneChan chan<- struct{}) sendBatchFn[*mockMessage] {
				once := sync.Once{}
				return func(ctx context.Context, b *Batch[*mockMessage]) error {
					defer once.Do(func() { doneChan <- struct{}{} })

					require.Len(t, b.messages, 1)
					require.Len(t, b.positions, 1)
					require.Equal(t, mockMsg(1), b.messages[0])
					require.Equal(t, testCommitPos, b.positions[0])
					return nil
				}
			},

			wantErr: context.Canceled,
		},
		{
			name: "ok - keep alive",
			msgs: []*WALMessage[*mockMessage]{
				NewWALMessage(&mockMessage{
					isEmptyFn: func() bool { return true },
				}, testCommitPos),
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(1), bytes)
					}
				},
			},
			sendFn: func(doneChan chan<- struct{}) sendBatchFn[*mockMessage] {
				once := sync.Once{}
				return func(ctx context.Context, b *Batch[*mockMessage]) error {
					defer once.Do(func() { doneChan <- struct{}{} })

					require.Len(t, b.messages, 0)
					require.Len(t, b.positions, 1)
					require.Equal(t, testCommitPos, b.positions[0])
					return nil
				}
			},

			wantErr: context.Canceled,
		},
		{
			name: "error - sending batch",
			msgs: []*WALMessage[*mockMessage]{
				testWALMsg(1),
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(1), bytes)
					}
				},
			},
			sendFn: func(doneChan chan<- struct{}) sendBatchFn[*mockMessage] {
				once := sync.Once{}
				return func(ctx context.Context, b *Batch[*mockMessage]) error {
					defer once.Do(func() { doneChan <- struct{}{} })
					return errTest
				}
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			sender, err := NewSender(&Config{
				BatchTimeout: 100 * time.Millisecond,
				MaxBatchSize: 10,
			}, tc.sendFn(doneChan), log.NewNoopLogger())
			require.NoError(t, err)
			defer sender.Close()

			sender.queueBytesSema = tc.semaphore

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := sender.Send(ctx)
				require.ErrorIs(t, err, tc.wantErr)
			}()

			for _, msg := range tc.msgs {
				sender.msgChan <- msg
			}

			for {
				select {
				case <-ctx.Done():
					t.Log("test timeout reached")
					wg.Wait()
					return
				case <-doneChan:
					if errors.Is(tc.wantErr, context.Canceled) {
						cancel()
					}
					wg.Wait()
					return
				}
			}
		})
	}
}
