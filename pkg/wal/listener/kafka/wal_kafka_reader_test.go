// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/backoff"
	backoffmocks "github.com/xataio/pgstream/internal/backoff/mocks"
	"github.com/xataio/pgstream/internal/kafka"
	kafkamocks "github.com/xataio/pgstream/internal/kafka/mocks"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestReader_Listen(t *testing.T) {
	t.Parallel()

	testMessage := &kafka.Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    1,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}

	testWalEvent := wal.Event{
		Data: &wal.Data{
			Action: "I",
			Schema: "test_schema",
			Table:  "test_table",
		},
		CommitPosition: wal.CommitPosition{
			KafkaPos: testMessage,
		},
	}

	errTest := errors.New("oh noes")

	testUnmarshaler := func(b []byte, a any) error {
		require.Equal(t, []byte("test-value"), b)
		data, ok := a.(*wal.Data)
		require.True(t, ok)
		*data = *testWalEvent.Data
		return nil
	}

	tests := []struct {
		name          string
		reader        func(doneChan chan struct{}) *kafkamocks.Reader
		processRecord payloadProcessor
		unmarshaler   func(b []byte, a any) error

		wantErr error
	}{
		{
			name: "ok",
			reader: func(doneChan chan struct{}) *kafkamocks.Reader {
				var once sync.Once
				return &kafkamocks.Reader{
					FetchMessageFn: func(ctx context.Context) (*kafka.Message, error) {
						defer once.Do(func() { doneChan <- struct{}{} })
						return testMessage, nil
					},
				}
			},
			processRecord: func(ctx context.Context, d *wal.Event) error {
				require.Equal(t, &testWalEvent, d)
				return nil
			},

			wantErr: context.Canceled,
		},
		{
			name: "error - fetching message",
			reader: func(doneChan chan struct{}) *kafkamocks.Reader {
				var once sync.Once
				return &kafkamocks.Reader{
					FetchMessageFn: func(ctx context.Context) (*kafka.Message, error) {
						defer once.Do(func() { doneChan <- struct{}{} })
						return nil, errTest
					},
				}
			},
			processRecord: func(ctx context.Context, d *wal.Event) error {
				return fmt.Errorf("processRecord: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - processing message",
			reader: func(doneChan chan struct{}) *kafkamocks.Reader {
				var once sync.Once
				return &kafkamocks.Reader{
					FetchMessageFn: func(ctx context.Context) (*kafka.Message, error) {
						defer once.Do(func() { doneChan <- struct{}{} })
						return testMessage, nil
					},
				}
			},
			processRecord: func(ctx context.Context, d *wal.Event) error {
				return errTest
			},

			wantErr: context.Canceled,
		},
		{
			name: "error - processing message context canceled",
			reader: func(doneChan chan struct{}) *kafkamocks.Reader {
				var once sync.Once
				return &kafkamocks.Reader{
					FetchMessageFn: func(ctx context.Context) (*kafka.Message, error) {
						defer once.Do(func() { doneChan <- struct{}{} })
						return testMessage, nil
					},
				}
			},
			processRecord: func(ctx context.Context, d *wal.Event) error {
				return context.Canceled
			},

			wantErr: context.Canceled,
		},
		{
			name: "error - unmarshaling message",
			reader: func(doneChan chan struct{}) *kafkamocks.Reader {
				var once sync.Once
				return &kafkamocks.Reader{
					FetchMessageFn: func(ctx context.Context) (*kafka.Message, error) {
						defer once.Do(func() { doneChan <- struct{}{} })
						return testMessage, nil
					},
				}
			},
			processRecord: func(ctx context.Context, d *wal.Event) error {
				return errors.New("processRecord: should not be called")
			},
			unmarshaler: func(b []byte, a any) error { return errTest },

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{}, 1)
			defer close(doneChan)

			r := &Reader{
				reader:        tc.reader(doneChan),
				processRecord: tc.processRecord,
				unmarshaler:   testUnmarshaler,
			}

			if tc.unmarshaler != nil {
				r.unmarshaler = tc.unmarshaler
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := r.Listen(ctx)
				require.ErrorIs(t, err, tc.wantErr)
			}()

			for {
				select {
				case <-doneChan:
					cancel()
					wg.Wait()
					return
				case <-ctx.Done():
					return
				}
			}
		})
	}
}

func TestReader_Checkpoint(t *testing.T) {
	t.Parallel()

	testMsgs := []*kafka.Message{
		{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value-1"),
		},
		{
			Key:   []byte("test-key-2"),
			Value: []byte("test-value-2"),
		},
	}

	testPositions := []wal.CommitPosition{
		{KafkaPos: testMsgs[0]},
		{KafkaPos: testMsgs[1]},
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name            string
		reader          *kafkamocks.Reader
		backoffProvider backoff.Provider

		wantErr error
	}{
		{
			name: "ok",
			reader: &kafkamocks.Reader{
				CommitMessagesFn: func(ctx context.Context, msgs ...*kafka.Message) error {
					require.ElementsMatch(t, msgs, testMsgs)
					return nil
				},
			},
			backoffProvider: func(ctx context.Context) backoff.Backoff {
				return &backoffmocks.Backoff{
					RetryNotifyFn: func(o backoff.Operation, n backoff.Notify) error {
						return o()
					},
				}
			},

			wantErr: nil,
		},
		{
			name: "error - committing messages",
			reader: &kafkamocks.Reader{
				CommitMessagesFn: func(ctx context.Context, msgs ...*kafka.Message) error {
					return errTest
				},
			},
			backoffProvider: func(ctx context.Context) backoff.Backoff {
				return &backoffmocks.Backoff{
					RetryNotifyFn: func(o backoff.Operation, n backoff.Notify) error {
						err := o()
						if err != nil {
							n(err, 50*time.Millisecond)
						}
						return err
					},
				}
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := Reader{
				reader:          tc.reader,
				backoffProvider: tc.backoffProvider,
			}

			err := r.Checkpoint(context.Background(), testPositions)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
