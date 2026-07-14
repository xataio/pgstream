// SPDX-License-Identifier: Apache-2.0

package batch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
				sendDone:          make(chan struct{}),
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
				// Simulate the post-fix shape: send() publishes the error
				// into the shared field before closing sendDone.
				batchSender.recordSendErr(errSendStopped)
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
				sendDone:          make(chan struct{}),
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
			sendDone:    make(chan struct{}),
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

// Regression test for https://github.com/xataio/pgstream/issues/372:
// concurrent SendMessage callers must all observe the underlying send error
// rather than wrapping a nil sendErr (which produced "%!w(<nil>)" messages
// that obscured the real cause of snapshot worker failures).
func TestSender_ConcurrentSendErrorPropagation(t *testing.T) {
	t.Parallel()

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
			return errTest
		}
	}

	sender, err := NewSender(ctx, &Config{
		BatchTimeout: 100 * time.Millisecond,
		MaxBatchSize: 1,
	}, sendFn(doneChan), log.NewNoopLogger())
	require.NoError(t, err)
	defer sender.Close()

	// prime the sender so the batch send fails
	require.NoError(t, sender.SendMessage(ctx, testWALMsg(1)))

	select {
	case <-doneChan:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for send to fail")
	}
	// Wait deterministically for send() to publish the error and close
	// sendDone — that's the exact "happens-before" we want every concurrent
	// SendMessage caller below to observe.
	select {
	case <-sender.sendDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for sendDone to close")
	}

	const workers = 8
	errs := make([]error, workers)
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = sender.SendMessage(ctx, testWALMsg(uint(i+2)))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.ErrorIsf(t, err, errSendStopped, "worker %d: missing errSendStopped", i)
		require.ErrorIsf(t, err, errTest, "worker %d: missing underlying send error", i)
		require.NotContainsf(t, err.Error(), "%!w(<nil>)", "worker %d: nil error wrapping leaked through", i)
	}
}

// TestSender_sendConcurrency verifies that with SendConcurrency > 1 the sender
// runs that many COPYs concurrently. The send function blocks until N sends are
// in flight at once; if the sender were serial this would deadlock and the test
// would hit its timeout.
func TestSender_sendConcurrency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const concurrency = 4
	testCommitPos := wal.CommitPosition("1")

	var active, maxActive atomic.Int32
	// barrier closes once `concurrency` sends are simultaneously in flight.
	var arrived atomic.Int32
	barrier := make(chan struct{})

	sendFn := func(_ context.Context, _ *Batch[*mockMessage]) error {
		n := active.Add(1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		if arrived.Add(1) == concurrency {
			close(barrier)
		}
		// wait until all concurrent drainers have arrived, proving they run in
		// parallel rather than serially.
		select {
		case <-barrier:
		case <-time.After(5 * time.Second):
		}
		active.Add(-1)
		return nil
	}

	sender, err := NewSender(ctx, &Config{
		BatchTimeout:    time.Minute,
		MaxBatchSize:    1,
		SendConcurrency: concurrency,
	}, sendFn, log.NewNoopLogger())
	require.NoError(t, err)
	defer sender.Close()

	for i := 0; i < concurrency; i++ {
		require.NoError(t, sender.SendMessage(ctx, NewWALMessage(&mockMessage{id: uint(i + 1)}, testCommitPos)))
	}

	select {
	case <-barrier:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %d concurrent sends (max observed: %d)", concurrency, maxActive.Load())
	}
	require.Equal(t, int32(concurrency), maxActive.Load())
	require.NoError(t, sender.Close())
}

// TestSender_multiDrainerFirstErrorWins verifies that when one of several
// drainers fails, the first error is surfaced, the in-flight COPYs in the other
// drainers are cancelled via the shared send-ctx, all drainers exit and Close
// returns the recorded error cleanly (no goroutine leak / no deadlock).
func TestSender_multiDrainerFirstErrorWins(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const concurrency = 4
	testCommitPos := wal.CommitPosition("1")
	errTest := errors.New("first error")

	var ctxCancelledCount atomic.Int32
	// the first send fails; the rest block until their ctx is cancelled by the
	// first error, then report the cancellation.
	sendFn := func(sendCtx context.Context, b *Batch[*mockMessage]) error {
		if b.messages[0].id == 1 {
			return errTest
		}
		select {
		case <-sendCtx.Done():
			ctxCancelledCount.Add(1)
			return sendCtx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}

	sender, err := NewSender(ctx, &Config{
		BatchTimeout:    time.Minute,
		MaxBatchSize:    1,
		SendConcurrency: concurrency,
	}, sendFn, log.NewNoopLogger())
	require.NoError(t, err)

	// send a handful of messages; at least one is the failing id==1.
	for i := 0; i < concurrency; i++ {
		id := uint(1)
		if i > 0 {
			id = uint(i + 10)
		}
		// once a send fails, SendMessage may start returning errSendStopped, so
		// don't assert on its error here.
		_ = sender.SendMessage(ctx, NewWALMessage(&mockMessage{id: id}, testCommitPos))
	}

	require.Eventually(t, func() bool {
		return errors.Is(sender.getSendErr(), errTest)
	}, 5*time.Second, time.Millisecond)

	// Close must return the first error and complete without leaking goroutines
	// or deadlocking.
	require.ErrorIs(t, sender.Close(), errTest)
}

// TestNewSender_autoTuneDisabledWithConcurrency verifies that the batch bytes
// auto-tuner is disabled (not merely ignored) when send concurrency > 1.
func TestNewSender_autoTuneDisabledWithConcurrency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	noopSendFn := func(context.Context, *Batch[*mockMessage]) error { return nil }

	t.Run("concurrency > 1 disables auto-tune", func(t *testing.T) {
		t.Parallel()
		sender, err := NewSender(ctx, &Config{
			SendConcurrency: 4,
			AutoTune:        AutoTuneConfig{Enabled: true},
		}, noopSendFn, log.NewNoopLogger())
		require.NoError(t, err)
		defer sender.Close()
		require.Nil(t, sender.batchBytesTuner)
	})

	t.Run("concurrency == 1 keeps auto-tune", func(t *testing.T) {
		t.Parallel()
		sender, err := NewSender(ctx, &Config{
			SendConcurrency: 1,
			AutoTune:        AutoTuneConfig{Enabled: true},
		}, noopSendFn, log.NewNoopLogger())
		require.NoError(t, err)
		defer sender.Close()
		require.NotNil(t, sender.batchBytesTuner)
	})
}

func TestConfig_GetSendConcurrency(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1, (&Config{}).GetSendConcurrency())
	require.Equal(t, 1, (&Config{SendConcurrency: 1}).GetSendConcurrency())
	require.Equal(t, 8, (&Config{SendConcurrency: 8}).GetSendConcurrency())
}

func TestSender_CloseAfterSendFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	errTest := errors.New("oh noes")
	testCommitPos := wal.CommitPosition("1")

	// When a batch send fails, the writer goroutine exits early and Close()
	// can end up closing the message channel while the batch message loop is
	// still selecting on it. The closed channel must stop the loop instead of
	// dereferencing a nil message, and Close must return the recorded send
	// error. Repeat to give the racing select a chance to pick the closed
	// message channel case.
	for i := 0; i < 50; i++ {
		sendFn := func(ctx context.Context, b *Batch[*mockMessage]) error {
			return errTest
		}
		sender, err := NewSender(ctx, &Config{
			BatchTimeout: 100 * time.Millisecond,
			MaxBatchSize: 1,
		}, sendFn, log.NewNoopLogger())
		require.NoError(t, err)

		require.NoError(t, sender.SendMessage(ctx, NewWALMessage(&mockMessage{id: 1}, testCommitPos)))

		// wait for the writer goroutine to record the send failure before
		// closing, so Close's wg.Wait returns while the batch message loop
		// may still be running
		require.Eventually(t, func() bool {
			return errors.Is(sender.getSendErr(), errTest)
		}, 5*time.Second, 100*time.Microsecond)

		require.ErrorIs(t, sender.Close(), errTest)
	}
}

// Regression test: constructing a sender and closing it immediately must not
// race the background send goroutine. Before the fix, s.cancelFn was assigned
// and s.wg.Add called inside the background goroutine, so an immediate Close
// could read the placeholder cancelFn and pass wg.Wait before Add ran
// (Add-after-Wait). Caught by the race detector.
func TestSender_CloseImmediately(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	noopSendFn := func(context.Context, *Batch[*mockMessage]) error { return nil }

	for i := 0; i < 100; i++ {
		sender, err := NewSender(ctx, &Config{
			BatchTimeout: 100 * time.Millisecond,
			MaxBatchSize: 1,
		}, noopSendFn, log.NewNoopLogger())
		require.NoError(t, err)
		require.NoError(t, sender.Close())
	}
}
