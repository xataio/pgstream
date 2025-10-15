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

func TestSender_SendMessage(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			batchSender := &Sender[*mockMessage]{
				batchSendInterval: 100 * time.Millisecond,
				maxBatchSize:      10,
				msgChan:           make(chan *WALMessage[*mockMessage]),
				queueBytesSema:    tc.weightedSemaphore,
				sendDone:          make(chan error, 1),
				once:              &sync.Once{},
				logger:            log.NewNoopLogger(),
				sendBatchFn:       noopSendFn,
				wg:                &sync.WaitGroup{},
				cancelFn:          func() {},
			}
			defer batchSender.Close()

			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if tc.sendDone {
				batchSender.sendDone <- errSendStopped
				close(batchSender.sendDone)
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer func() {
					batchSender.Close()
					wg.Done()
				}()
				err := batchSender.SendMessage(ctx, tc.msg)
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

func TestSender_send(t *testing.T) {
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
		name         string
		msgs         []*WALMessage[*mockMessage]
		semaphore    *syncmocks.WeightedSemaphore
		sendFn       func(doneChan chan<- struct{}) sendBatchFn[*mockMessage]
		ignoreErrors bool

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
			name: "ok - error ignored sending batch",
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
			ignoreErrors: true,

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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			sender := &Sender[*mockMessage]{
				batchSendInterval: 100 * time.Millisecond,
				maxBatchSize:      10,
				msgChan:           make(chan *WALMessage[*mockMessage]),
				queueBytesSema:    tc.semaphore,
				sendDone:          make(chan error, 1),
				once:              &sync.Once{},
				logger:            log.NewNoopLogger(),
				sendBatchFn:       tc.sendFn(doneChan),
				wg:                &sync.WaitGroup{},
				cancelFn:          func() {},
				ignoreSendErrors:  tc.ignoreErrors,
			}
			defer sender.Close()

			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := sender.send(ctx)
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

	t.Run("graceful shutdown, drain in-flight batch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		doneChan := make(chan struct{}, 1)
		defer close(doneChan)

		sendFn := func(doneChan chan<- struct{}) sendBatchFn[*mockMessage] {
			once := sync.Once{}
			return func(ctx context.Context, b *Batch[*mockMessage]) error {
				defer once.Do(func() { doneChan <- struct{}{} })

				require.Len(t, b.messages, 1)
				require.Len(t, b.positions, 1)
				require.Equal(t, mockMsg(1), b.messages[0])
				require.Equal(t, testCommitPos, b.positions[0])
				return nil
			}
		}

		sender := &Sender[*mockMessage]{
			batchSendInterval: 1 * time.Minute,
			maxBatchSize:      10,
			msgChan:           make(chan *WALMessage[*mockMessage]),
			queueBytesSema: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(1), bytes)
					}
				},
			},
			sendDone:    make(chan error, 1),
			once:        &sync.Once{},
			logger:      log.NewNoopLogger(),
			sendBatchFn: sendFn(doneChan),
			wg:          &sync.WaitGroup{},
			cancelFn:    func() {},
		}
		defer sender.Close()

		ctx, cancel := context.WithCancel(ctx)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := sender.send(ctx)
			require.ErrorIs(t, err, context.Canceled)
		}()

		sender.msgChan <- testWALMsg(1)
		cancel()
		wg.Wait()
	})
}

func TestSender(t *testing.T) {
	ctx := context.Background()

	errTest := errors.New("oh noes")
	testCommitPos := wal.CommitPosition("1")

	mockMsg := func(i uint) *mockMessage {
		return &mockMessage{id: i}
	}

	testWALMsg := func(i uint) *WALMessage[*mockMessage] {
		return NewWALMessage(mockMsg(i), testCommitPos)
	}

	doneChan := make(chan struct{}, 1)
	defer close(doneChan)

	sendFn := func(doneChan chan<- struct{}) sendBatchFn[*mockMessage] {
		once := sync.Once{}
		return func(ctx context.Context, b *Batch[*mockMessage]) error {
			defer once.Do(func() { doneChan <- struct{}{} })

			require.Len(t, b.messages, 1)
			require.Len(t, b.positions, 1)
			require.Equal(t, mockMsg(1), b.messages[0])
			require.Equal(t, testCommitPos, b.positions[0])
			return errTest
		}
	}

	sender, err := NewSender(ctx, &Config{
		BatchTimeout: 100 * time.Millisecond,
		MaxBatchSize: 1,
	}, sendFn(doneChan), log.NewNoopLogger())
	require.NoError(t, err)
	defer sender.Close()

	err = sender.SendMessage(ctx, testWALMsg(1))
	require.NoError(t, err)

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-doneChan:
			// give time for the error to cascade
			time.Sleep(100 * time.Millisecond)
			err = sender.SendMessage(ctx, testWALMsg(2))
			require.ErrorIs(t, err, errSendStopped)
			sender.Close()
			return
		case <-timer.C:
			t.Error("test timeout")
			return
		}
	}
}
