// SPDX-License-Identifier: Apache-2.0

package natsjetstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/pkg/backoff"
	backoffmocks "github.com/xataio/pgstream/pkg/backoff/mocks"
	natslib "github.com/xataio/pgstream/pkg/nats"
	natsmocks "github.com/xataio/pgstream/pkg/nats/mocks"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestCheckpointer_CommitOffsets(t *testing.T) {
	t.Parallel()

	testPositions := []wal.CommitPosition{
		"stream_1/consumer_1/1", "stream_1/consumer_1/2", "stream_1/consumer_2/1",
	}
	testOffsets := []*natslib.Offset{
		{
			Stream:    "stream_1",
			Consumer:  "consumer_1",
			StreamSeq: 1,
		},
		{
			Stream:    "stream_1",
			Consumer:  "consumer_1",
			StreamSeq: 2,
		},
		{
			Stream:    "stream_1",
			Consumer:  "consumer_2",
			StreamSeq: 1,
		},
	}

	mockParser := &natsmocks.OffsetParser{
		FromStringFn: func(s string) (*natslib.Offset, error) {
			switch s {
			case string(testPositions[0]):
				return testOffsets[0], nil
			case string(testPositions[1]):
				return testOffsets[1], nil
			case string(testPositions[2]):
				return testOffsets[2], nil
			default:
				return nil, errors.New("invalid offset format")
			}
		},
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name            string
		committer       *natsmocks.Reader
		backoffProvider backoff.Provider
		parser          natslib.OffsetParser

		wantErr error
	}{
		{
			name: "ok",
			committer: &natsmocks.Reader{
				CommitOffsetsFn: func(ctx context.Context, offsets ...*natslib.Offset) error {
					// should receive 2 offsets: highest per stream+consumer
					// stream_1-consumer_1 -> seq 2, stream_1-consumer_2 -> seq 1
					require.Len(t, offsets, 2)
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
			committer: &natsmocks.Reader{
				CommitOffsetsFn: func(ctx context.Context, offsets ...*natslib.Offset) error {
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
			committer: &natsmocks.Reader{
				CommitOffsetsFn: func(ctx context.Context, offsets ...*natslib.Offset) error {
					return errors.New("CommitOffsetsFn: should not be called")
				},
			},
			parser: &natsmocks.OffsetParser{
				FromStringFn: func(s string) (*natslib.Offset, error) { return nil, errTest },
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := Checkpointer{
				logger:          loglib.NewNoopLogger(),
				committer:       tc.committer,
				backoffProvider: tc.backoffProvider,
				offsetParser:    mockParser,
			}

			if tc.parser != nil {
				c.offsetParser = tc.parser
			}

			err := c.CommitOffsets(context.Background(), testPositions)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
