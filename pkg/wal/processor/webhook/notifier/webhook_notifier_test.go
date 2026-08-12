// SPDX-License-Identifier: Apache-2.0

package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	httplib "github.com/xataio/pgstream/internal/http"
	httpmocks "github.com/xataio/pgstream/internal/http/mocks"
	syncmocks "github.com/xataio/pgstream/internal/sync/mocks"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/store/mocks"
)

func TestNotifier_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	testEvent := &wal.Event{
		Data: &wal.Data{
			Action: "I",
			Schema: "test_schema",
			Table:  "test_table",
		},
		CommitPosition: testCommitPos,
	}

	testSubscription := func(url string) *subscription.Subscription {
		return newTestSubscription(url, "", "", nil)
	}

	testPayload, err := json.Marshal(&webhook.Payload{Data: testEvent.Data})
	require.NoError(t, err)

	tests := []struct {
		name              string
		store             subscriptionRetriever
		event             *wal.Event
		serialiser        func(any) ([]byte, error)
		weightedSemaphore *syncmocks.WeightedSemaphore

		wantMsgs []*notifyMsg
		wantErr  error
	}{
		{
			name: "ok - no subscriptions for event",
			store: &mocks.Store{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
					return []*subscription.Subscription{}, nil
				},
			},
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool {
					require.Equal(t, int64(0), i)
					return true
				},
			},
			event: testEvent,

			wantMsgs: []*notifyMsg{testNotifyMsg([]string{}, nil)},
			wantErr:  nil,
		},
		{
			name: "ok - subscriptions for event",
			store: &mocks.Store{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
					return []*subscription.Subscription{
						testSubscription("url-1"), testSubscription("url-2"),
					}, nil
				},
			},
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool {
					require.Equal(t, int64(len(testPayload)+len("url-1")+len("url-2")), i)
					return true
				},
			},
			event: testEvent,

			wantMsgs: []*notifyMsg{
				testNotifyMsg([]string{"url-1", "url-2"}, testPayload),
			},
			wantErr: nil,
		},
		{
			name: "error - getting subscriptions",
			store: &mocks.Store{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
					return nil, errTest
				},
			},
			event: testEvent,

			wantMsgs: []*notifyMsg{},
			wantErr:  errTest,
		},
		{
			name: "error - serialising payload",
			store: &mocks.Store{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
					return []*subscription.Subscription{
						testSubscription("url-1"), testSubscription("url-2"),
					}, nil
				},
			},
			serialiser: func(a any) ([]byte, error) { return nil, errTest },
			event:      testEvent,

			wantMsgs: []*notifyMsg{},
			wantErr:  errTest,
		},
		{
			name: "error - acquiring semaphore",
			store: &mocks.Store{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
					return []*subscription.Subscription{
						testSubscription("url-1"), testSubscription("url-2"),
					}, nil
				},
			},
			weightedSemaphore: &syncmocks.WeightedSemaphore{
				TryAcquireFn: func(i int64) bool { return false },
				AcquireFn:    func(ctx context.Context, i int64) error { return errTest },
			},
			event: testEvent,

			wantMsgs: []*notifyMsg{},
			wantErr:  errTest,
		},
		{
			name: "error - panic recovery",
			store: &mocks.Store{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
					panic(errTest)
				},
			},
			event: testEvent,

			wantMsgs: []*notifyMsg{},
			wantErr:  processor.ErrPanic,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			n := New(&Config{}, tc.store)
			if tc.serialiser != nil {
				n.serialiser = tc.serialiser
			}

			if tc.weightedSemaphore != nil {
				n.queueBytesSema = tc.weightedSemaphore
			}

			errCh := make(chan error, 1)
			go func() {
				errCh <- n.ProcessWALEvent(context.Background(), tc.event)
			}()

			// unbuffered send needs a receiver
			msgs := []*notifyMsg{}
			for i := 0; i < len(tc.wantMsgs); i++ {
				select {
				case msg := <-n.notifyChan:
					msgs = append(msgs, msg)
				case <-time.After(time.Second):
					t.Fatalf("timed out waiting for notifyChan message %d", i)
				}
			}
			require.ErrorIs(t, <-errCh, tc.wantErr)
			require.Equal(t, tc.wantMsgs, msgs)
		})
	}
}

func TestNotifier_Notify(t *testing.T) {
	t.Parallel()

	testPayload := []byte("test payload")
	url1 := "url-1"
	url2 := "url-2"

	testCfg := &Config{
		URLWorkerCount: 2,
		// keep retries fast in tests
		Backoff: backoff.Config{
			Exponential: &backoff.ExponentialConfig{
				InitialInterval: time.Millisecond,
				MaxInterval:     2 * time.Millisecond,
				MaxRetries:      2,
			},
		},
	}

	tests := []struct {
		name         string
		strictMode   bool
		semaphore    *syncmocks.WeightedSemaphore
		client       httplib.Client
		msgs         []*notifyMsg
		checkpointer func(chan struct{}) checkpointer.Checkpoint

		wantErr error
	}{
		{
			name: "ok",
			client: &httpmocks.Client{
				DoFn: func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == url1 || r.URL.Path == url2 {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       http.NoBody,
						}, nil
					}
					return nil, fmt.Errorf("unexpected request url: %v", r.URL)
				},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(len(testPayload)), bytes)
					}
				},
			},
			msgs: []*notifyMsg{
				testNotifyMsg([]string{url1, url2}, testPayload),
			},
			checkpointer: func(doneChan chan struct{}) checkpointer.Checkpoint {
				return func(ctx context.Context, positions []wal.CommitPosition) error {
					defer func() {
						doneChan <- struct{}{}
					}()
					require.Equal(t, []wal.CommitPosition{testCommitPos}, positions)
					return nil
				}
			},

			wantErr: context.Canceled,
		},
		{
			// failed delivery must skip checkpoint
			name: "error - sending webhook, checkpoint not called",
			client: &httpmocks.Client{
				DoFn: func(r *http.Request) (*http.Response, error) {
					return nil, errTest
				},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {
					if i == 0 {
						require.Equal(t, int64(len(testPayload)), bytes)
					}
				},
			},
			msgs: []*notifyMsg{
				testNotifyMsg([]string{url1}, testPayload),
			},
			checkpointer: func(doneChan chan struct{}) checkpointer.Checkpoint {
				return func(ctx context.Context, positions []wal.CommitPosition) error {
					doneChan <- struct{}{}
					t.Error("checkpointer should not be called when webhook delivery fails")
					return nil
				}
			},

			wantErr: errTest,
		},
		{
			name: "error - partial failure, one url transient-fails, checkpoint not called",
			client: &httpmocks.Client{
				DoFn: func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == url1 {
						return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
					}
					return nil, errTest
				},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {},
			},
			msgs: []*notifyMsg{
				testNotifyMsg([]string{url1, url2}, testPayload),
			},
			checkpointer: func(doneChan chan struct{}) checkpointer.Checkpoint {
				return func(ctx context.Context, positions []wal.CommitPosition) error {
					doneChan <- struct{}{}
					t.Error("checkpointer should not be called when any webhook delivery still fails after retries")
					return nil
				}
			},

			wantErr: errTest,
		},
		{
			// 4xx must not block others
			name: "ok - permanent failure on one url does not block checkpoint",
			client: &httpmocks.Client{
				DoFn: func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == url1 {
						return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
					}
					return &http.Response{StatusCode: http.StatusBadRequest, Body: http.NoBody}, nil
				},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {},
			},
			msgs: []*notifyMsg{
				testNotifyMsg([]string{url1, url2}, testPayload),
			},
			checkpointer: func(doneChan chan struct{}) checkpointer.Checkpoint {
				return func(ctx context.Context, positions []wal.CommitPosition) error {
					defer func() {
						doneChan <- struct{}{}
					}()
					require.Equal(t, []wal.CommitPosition{testCommitPos}, positions)
					return nil
				}
			},

			wantErr: context.Canceled,
		},
		{
			// strict_mode: permanent failures block too
			name:       "error - strict mode blocks checkpoint on permanent failure",
			strictMode: true,
			client: &httpmocks.Client{
				DoFn: func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == url1 {
						return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
					}
					return &http.Response{StatusCode: http.StatusBadRequest, Body: http.NoBody}, nil
				},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {},
			},
			msgs: []*notifyMsg{
				testNotifyMsg([]string{url1, url2}, testPayload),
			},
			checkpointer: func(doneChan chan struct{}) checkpointer.Checkpoint {
				return func(ctx context.Context, positions []wal.CommitPosition) error {
					doneChan <- struct{}{}
					t.Error("checkpointer should not be called in strict mode on permanent failure")
					return nil
				}
			},

			wantErr: backoff.ErrPermanent,
		},
		{
			name: "error - checkpointing",
			client: &httpmocks.Client{
				DoFn: func(r *http.Request) (*http.Response, error) {
					return nil, errors.New("DoFn: should not be called")
				},
			},
			semaphore: &syncmocks.WeightedSemaphore{
				ReleaseFn: func(i uint64, bytes int64) {},
			},
			msgs: []*notifyMsg{
				testNotifyMsg([]string{}, nil),
			},
			checkpointer: func(doneChan chan struct{}) checkpointer.Checkpoint {
				return func(ctx context.Context, positions []wal.CommitPosition) error {
					defer func() {
						doneChan <- struct{}{}
					}()
					require.Equal(t, []wal.CommitPosition{testCommitPos}, positions)
					return errTest
				}
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			n := New(testCfg, &mocks.Store{})
			n.client = tc.client
			n.queueBytesSema = tc.semaphore
			n.checkpointer = tc.checkpointer(doneChan)
			n.strictMode = tc.strictMode

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			notifyErrCh := make(chan error, 1)
			go func() {
				notifyErrCh <- n.Notify(ctx)
			}()

			for _, msg := range tc.msgs {
				n.notifyChan <- msg
			}

		loop:
			for {
				select {
				case <-ctx.Done():
					t.Log("test timeout reached")
					break loop
				case <-doneChan:
					if errors.Is(tc.wantErr, context.Canceled) {
						cancel()
					}
				case err := <-notifyErrCh:
					require.ErrorIs(t, err, tc.wantErr)
					return
				}
			}

			err := <-notifyErrCh
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestNotifier_sendWebhook(t *testing.T) {
	t.Parallel()

	testCfg := &Config{
		Backoff: backoff.Config{
			Exponential: &backoff.ExponentialConfig{
				InitialInterval: time.Millisecond,
				MaxInterval:     2 * time.Millisecond,
				MaxRetries:      2,
			},
		},
	}

	tests := []struct {
		name          string
		lsn           string
		doFn          func(callCount *int) func(*http.Request) (*http.Response, error)
		wantErr       error
		wantPermanent bool
		wantCalls     int
	}{
		{
			name: "ok - success on first try, headers set",
			lsn:  "test-lsn",
			doFn: func(callCount *int) func(*http.Request) (*http.Response, error) {
				return func(r *http.Request) (*http.Response, error) {
					*callCount++
					require.Equal(t, "test-lsn", r.Header.Get("X-Pgstream-LSN"))
					require.Equal(t, "test-lsn", r.Header.Get("Idempotency-Key"))
					return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
				}
			},
			wantErr:   nil,
			wantCalls: 1,
		},
		{
			name: "ok - 202 accepted is success, not permanent failure",
			lsn:  "test-lsn",
			doFn: func(callCount *int) func(*http.Request) (*http.Response, error) {
				return func(r *http.Request) (*http.Response, error) {
					*callCount++
					return &http.Response{StatusCode: http.StatusAccepted, Body: http.NoBody}, nil
				}
			},
			wantErr:   nil,
			wantCalls: 1,
		},
		{
			name: "ok - 204 no content is success, headers omitted for zero lsn",
			lsn:  wal.ZeroLSN,
			doFn: func(callCount *int) func(*http.Request) (*http.Response, error) {
				return func(r *http.Request) (*http.Response, error) {
					*callCount++
					require.Empty(t, r.Header.Get("X-Pgstream-LSN"))
					require.Empty(t, r.Header.Get("Idempotency-Key"))
					return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
				}
			},
			wantErr:   nil,
			wantCalls: 1,
		},
		{
			name: "ok - network error retried until success",
			lsn:  "test-lsn",
			doFn: func(callCount *int) func(*http.Request) (*http.Response, error) {
				return func(r *http.Request) (*http.Response, error) {
					*callCount++
					if *callCount < 2 {
						return nil, errTest
					}
					return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
				}
			},
			wantErr:   nil,
			wantCalls: 2,
		},
		{
			name: "ok - 5xx retried until success",
			lsn:  "test-lsn",
			doFn: func(callCount *int) func(*http.Request) (*http.Response, error) {
				return func(r *http.Request) (*http.Response, error) {
					*callCount++
					if *callCount < 2 {
						return &http.Response{StatusCode: http.StatusInternalServerError, Body: http.NoBody}, nil
					}
					return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
				}
			},
			wantErr:   nil,
			wantCalls: 2,
		},
		{
			name: "ok - 429 retried until success",
			lsn:  "test-lsn",
			doFn: func(callCount *int) func(*http.Request) (*http.Response, error) {
				return func(r *http.Request) (*http.Response, error) {
					*callCount++
					if *callCount < 2 {
						return &http.Response{StatusCode: http.StatusTooManyRequests, Body: http.NoBody}, nil
					}
					return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
				}
			},
			wantErr:   nil,
			wantCalls: 2,
		},
		{
			name: "error - 4xx is not retried",
			lsn:  "test-lsn",
			doFn: func(callCount *int) func(*http.Request) (*http.Response, error) {
				return func(r *http.Request) (*http.Response, error) {
					*callCount++
					return &http.Response{StatusCode: http.StatusBadRequest, Body: http.NoBody}, nil
				}
			},
			wantPermanent: true,
			wantCalls:     1,
		},
		{
			name: "error - persistent network error exhausts retries",
			lsn:  "test-lsn",
			doFn: func(callCount *int) func(*http.Request) (*http.Response, error) {
				return func(r *http.Request) (*http.Response, error) {
					*callCount++
					return nil, errTest
				}
			},
			wantErr:   errTest,
			wantCalls: 3, // 1 attempt + 2 retries
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			callCount := 0
			n := New(testCfg, &mocks.Store{})
			n.client = &httpmocks.Client{DoFn: tc.doFn(&callCount)}

			err := n.sendWebhook(context.Background(), []byte("payload"), tc.lsn, "url-1")
			if tc.wantPermanent {
				require.ErrorIs(t, err, backoff.ErrPermanent)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Equal(t, tc.wantCalls, callCount)
		})
	}
}

func TestNotifier(t *testing.T) {
	t.Parallel()
	n := New(&Config{}, &mocks.Store{
		GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
			return []*subscription.Subscription{newTestSubscription("url-1", "", "", nil)}, nil
		},
	})
	n.client = &httpmocks.Client{
		DoFn: func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		},
	}
	n.checkpointer = func(ctx context.Context, positions []wal.CommitPosition) error {
		return errTest
	}

	doneChan := make(chan struct{}, 1)
	go func() {
		err := n.Notify(context.Background())
		require.ErrorIs(t, err, errTest)
		doneChan <- struct{}{}
		close(doneChan)
	}()

	walEvent := &wal.Event{
		CommitPosition: wal.CommitPosition("1"),
		Data: &wal.Data{
			Action: "I",
		},
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-doneChan:
			// assert fresh call, not stale
			require.ErrorIs(t, n.ProcessWALEvent(context.Background(), walEvent), errTest)
			return
		case <-timer.C:
			t.Error("test timeout")
			return
		default:
			_ = n.ProcessWALEvent(context.Background(), walEvent)
		}
	}
}

// regression: closing notifyChan caused panics
func TestNotifier_NotifyAfterClose(t *testing.T) {
	t.Parallel()

	n := New(&Config{}, &mocks.Store{})

	errChan := make(chan error, 1)
	go func() {
		errChan <- n.Notify(context.Background())
	}()

	// works whether Notify started yet
	require.NoError(t, n.Close())

	select {
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Notify did not return after Close")
	}
}

// regression: no closed-channel send panic
func TestNotifier_ProcessWALEventDuringClose(t *testing.T) {
	t.Parallel()

	n := New(&Config{}, &mocks.Store{
		GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
			return []*subscription.Subscription{}, nil
		},
	})

	// must not panic before Notify runs
	require.NoError(t, n.Close())

	const workers = 8
	errs := make([]error, workers)
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = n.ProcessWALEvent(context.Background(), &wal.Event{
				CommitPosition: wal.CommitPosition(fmt.Sprintf("w-%d", i)),
				Data:           &wal.Data{Action: "I"},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.ErrorIsf(t, err, errNotifyStopped, "worker %d: missing errNotifyStopped", i)
	}
}

// regression: avoid nil error wrapping
func TestNotifier_ConcurrentProcessWALEventErrorPropagation(t *testing.T) {
	t.Parallel()

	n := New(&Config{}, &mocks.Store{
		GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
			return []*subscription.Subscription{newTestSubscription("url-1", "", "", nil)}, nil
		},
	})
	n.client = &httpmocks.Client{
		DoFn: func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		},
	}
	n.checkpointer = func(ctx context.Context, positions []wal.CommitPosition) error {
		return errTest
	}

	notifyDone := make(chan struct{})
	go func() {
		defer close(notifyDone)
		err := n.Notify(context.Background())
		require.ErrorIs(t, err, errTest)
	}()

	// seed message, make Notify exit
	require.NoError(t, n.ProcessWALEvent(context.Background(), &wal.Event{
		CommitPosition: wal.CommitPosition("seed"),
		Data:           &wal.Data{Action: "I"},
	}))

	// no sleep: ordering guarantees visibility
	select {
	case <-notifyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Notify to fail")
	}

	const workers = 8
	errs := make([]error, workers)
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = n.ProcessWALEvent(context.Background(), &wal.Event{
				CommitPosition: wal.CommitPosition(fmt.Sprintf("w-%d", i)),
				Data:           &wal.Data{Action: "I"},
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.ErrorIsf(t, err, errNotifyStopped, "worker %d: missing errNotifyStopped", i)
		require.ErrorIsf(t, err, errTest, "worker %d: missing underlying notify error", i)
		require.NotContainsf(t, err.Error(), "%!w(<nil>)", "worker %d: nil error wrapping leaked through", i)
	}
}
