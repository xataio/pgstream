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
	"github.com/xataio/pgstream/pkg/kafka"
	kafkamocks "github.com/xataio/pgstream/pkg/kafka/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestReader_Listen(t *testing.T) {
	t.Parallel()

	testOffsetStr := "test-topic/0/1"
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
		CommitPosition: wal.CommitPosition(testOffsetStr),
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
				logger:        loglib.NewNoopLogger(),
				reader:        tc.reader(doneChan),
				processRecord: tc.processRecord,
				unmarshaler:   testUnmarshaler,
				offsetParser: &kafkamocks.OffsetParser{
					ToStringFn: func(o *kafka.Offset) string { return testOffsetStr },
				},
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
