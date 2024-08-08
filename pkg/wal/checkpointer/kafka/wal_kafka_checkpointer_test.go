// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/pkg/backoff"
	backoffmocks "github.com/xataio/pgstream/pkg/backoff/mocks"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkamocks "github.com/xataio/pgstream/pkg/kafka/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestCheckpointer_CommitOffsets(t *testing.T) {
	t.Parallel()

	testPositions := []wal.CommitPosition{
		"topic_1-0-1", "topic_1-1-1", "topic_1-1-2",
	}
	testOffsets := []*kafka.Offset{
		{
			Topic:     "topic_1",
			Partition: 0,
			Offset:    1,
		},
		{
			Topic:     "topic_1",
			Partition: 1,
			Offset:    1,
		},
		{
			Topic:     "topic_1",
			Partition: 1,
			Offset:    2,
		},
	}

	mockParser := &kafkamocks.OffsetParser{
		FromStringFn: func(s string) (*kafka.Offset, error) {
			switch s {
			case string(testPositions[0]):
				return testOffsets[0], nil
			case string(testPositions[1]):
				return testOffsets[1], nil
			case string(testPositions[2]):
				return testOffsets[2], nil
			default:
				return nil, kafka.ErrInvalidOffsetFormat
			}
		},
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name            string
		reader          *kafkamocks.Reader
		backoffProvider backoff.Provider
		parser          kafka.OffsetParser

		wantErr error
	}{
		{
			name: "ok",
			reader: &kafkamocks.Reader{
				CommitOffsetsFn: func(ctx context.Context, offsets ...*kafka.Offset) error {
					require.ElementsMatch(t, offsets, []*kafka.Offset{
						testOffsets[0], testOffsets[2],
					})
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
			name: "error - committing offsets",
			reader: &kafkamocks.Reader{
				CommitOffsetsFn: func(ctx context.Context, offsets ...*kafka.Offset) error {
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
		{
			name: "error - parsing offsets",
			reader: &kafkamocks.Reader{
				CommitOffsetsFn: func(ctx context.Context, offsets ...*kafka.Offset) error {
					return errors.New("CommitOffsetsFn: should not be called")
				},
			},
			parser: &kafkamocks.OffsetParser{
				FromStringFn: func(s string) (*kafka.Offset, error) { return nil, errTest },
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
				offsetParser:    mockParser,
			}

			if tc.parser != nil {
				r.offsetParser = tc.parser
			}

			err := r.CommitOffsets(context.Background(), testPositions)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
