// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/backoff"
	backoffmocks "github.com/xataio/pgstream/internal/backoff/mocks"
	"github.com/xataio/pgstream/internal/kafka"
	kafkamocks "github.com/xataio/pgstream/internal/kafka/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestCheckpointer_CommitMessages(t *testing.T) {
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

			r := Checkpointer{
				logger:          loglib.NewNoopLogger(),
				committer:       tc.reader,
				backoffProvider: tc.backoffProvider,
			}

			err := r.CommitMessages(context.Background(), testPositions)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
