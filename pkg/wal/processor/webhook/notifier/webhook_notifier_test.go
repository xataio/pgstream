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
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/mocks"
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

	testSubscription := func(url string) *webhook.Subscription {
		return newTestSubscription(url, "", "", nil)
	}

	testPayload, err := json.Marshal(&NotifyPayload{Data: testEvent.Data})
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
			store: &mocks.SubscriptionStore{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error) {
					return []*webhook.Subscription{}, nil
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
			store: &mocks.SubscriptionStore{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error) {
					return []*webhook.Subscription{
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
			store: &mocks.SubscriptionStore{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error) {
					return nil, errTest
				},
			},
			event: testEvent,

			wantMsgs: []*notifyMsg{},
			wantErr:  errTest,
		},
		{
			name: "error - serialising payload",
			store: &mocks.SubscriptionStore{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error) {
					return []*webhook.Subscription{
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
			store: &mocks.SubscriptionStore{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error) {
					return []*webhook.Subscription{
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
			store: &mocks.SubscriptionStore{
				GetSubscriptionsFn: func(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error) {
					panic(errTest)
				},
			},
			event: testEvent,

			wantMsgs: []*notifyMsg{},
			wantErr:  processor.ErrPanic,
		},
	}

	for _, tc := range tests {
		tc := tc
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
				close(n.notifyChan)
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			n := New(testCfg, &mocks.SubscriptionStore{})
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
