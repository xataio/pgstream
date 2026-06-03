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

			go func() {
				err := n.ProcessWALEvent(context.Background(), tc.event)
				require.ErrorIs(t, err, tc.wantErr)
				n.closeNotifyChan()
			}()

			msgs := []*notifyMsg{}
			for msg := range n.notifyChan {
				msgs = append(msgs, msg)
			}
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
	}

	tests := []struct {
		name         string
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
			name: "ok - error sending webhook",
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

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := n.Notify(ctx)
				require.ErrorIs(t, err, tc.wantErr)
			}()

			for _, msg := range tc.msgs {
				n.notifyChan <- msg
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

func TestNotifier(t *testing.T) {
	t.Parallel()
	n := New(&Config{}, &mocks.Store{
		GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
			return []*subscription.Subscription{newTestSubscription("url-1", "", "", nil)}, nil
		},
	})
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

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var processErr error
	for {
		select {
		case <-doneChan:
			require.ErrorIs(t, processErr, errTest)
			return
		case <-timer.C:
			t.Error("test timeout")
			return
		default:
			processErr = n.ProcessWALEvent(context.Background(), &wal.Event{
				CommitPosition: wal.CommitPosition("1"),
				Data: &wal.Data{
					Action: "I",
				},
			})
		}
	}
}

// Regression test: Notify must not dereference a nil msg when notifyChan is
// closed via Close(). Prior to the fix, Close()-during-Notify produced a
// "invalid memory address or nil pointer dereference" panic at notify().
func TestNotifier_NotifyAfterClose(t *testing.T) {
	t.Parallel()

	n := New(&Config{}, &mocks.Store{})

	errChan := make(chan error, 1)
	go func() {
		errChan <- n.Notify(context.Background())
	}()

	// give Notify a moment to enter its select
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, n.Close())

	select {
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Notify did not return after Close")
	}
}

// Regression test: concurrent ProcessWALEvent callers must all observe the
// underlying Notify error rather than wrapping a nil notifyErr (which would
// produce "%!w(<nil>)" messages, the same pattern fixed for the batch sender
// in issue #372).
func TestNotifier_ConcurrentProcessWALEventErrorPropagation(t *testing.T) {
	t.Parallel()

	n := New(&Config{}, &mocks.Store{
		GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
			return []*subscription.Subscription{newTestSubscription("url-1", "", "", nil)}, nil
		},
	})
	n.checkpointer = func(ctx context.Context, positions []wal.CommitPosition) error {
		return errTest
	}

	notifyDone := make(chan struct{})
	go func() {
		defer close(notifyDone)
		err := n.Notify(context.Background())
		require.ErrorIs(t, err, errTest)
	}()

	// seed a message that will make Notify exit with errTest
	require.NoError(t, n.ProcessWALEvent(context.Background(), &wal.Event{
		CommitPosition: wal.CommitPosition("seed"),
		Data:           &wal.Data{Action: "I"},
	}))

	select {
	case <-notifyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Notify to fail")
	}
	// give time for notifyDone close to propagate
	time.Sleep(100 * time.Millisecond)

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
