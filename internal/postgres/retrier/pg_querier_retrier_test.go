// SPDX-License-Identifier: Apache-2.0

package retrier

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
)

func TestQuerier_Query(t *testing.T) {
	t.Parallel()

	retriableErr := errors.New("connection failed")
	nonRetriableErr := context.Canceled

	tests := []struct {
		name    string
		querier postgres.Querier

		wantRowsFieldDescriptions []pgconn.FieldDescription
		wantErr                   error
	}{
		{
			name: "ok",
			querier: &mocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (postgres.Rows, error) {
					return &mocks.Rows{
						FieldDescriptionsFn: func() []pgconn.FieldDescription {
							return []pgconn.FieldDescription{
								{Name: "id"}, {Name: "name"},
							}
						},
					}, nil
				},
			},
			wantRowsFieldDescriptions: []pgconn.FieldDescription{
				{Name: "id"}, {Name: "name"},
			},
			wantErr: nil,
		},
		{
			name: "error - non-retriable",
			querier: &mocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (postgres.Rows, error) {
					return nil, nonRetriableErr
				},
			},
			wantRowsFieldDescriptions: nil,
			wantErr:                   nonRetriableErr,
		},
		{
			name: "error - retriable but always fails",
			querier: &mocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (postgres.Rows, error) {
					return nil, retriableErr
				},
			},
			wantRowsFieldDescriptions: nil,
			wantErr:                   retriableErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := &Querier{
				connBuilder: func(ctx context.Context) (postgres.Querier, error) {
					return tc.querier, nil
				},
				querier:         tc.querier,
				backoffProvider: newMockBackoffProvider(),
				logger:          loglib.NewNoopLogger(),
			}

			gotRows, gotErr := q.Query(context.Background(), "SELECT 1")
			require.ErrorIs(t, gotErr, tc.wantErr)
			if tc.wantRowsFieldDescriptions != nil {
				require.NotNil(t, gotRows)
				require.Equal(t, tc.wantRowsFieldDescriptions, gotRows.FieldDescriptions())
			}
		})
	}
}

func TestQuerier_QueryRow(t *testing.T) {
	t.Parallel()

	retriableErr := errors.New("connection failed")
	nonRetriableErr := context.Canceled

	tests := []struct {
		name    string
		querier postgres.Querier
		wantErr error
	}{
		{
			name: "ok",
			querier: &mocks.Querier{
				QueryRowFn: func(_ context.Context, _ []any, _ string, _ ...any) error {
					return nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error - non-retriable",
			querier: &mocks.Querier{
				QueryRowFn: func(_ context.Context, _ []any, _ string, _ ...any) error {
					return nonRetriableErr
				},
			},
			wantErr: nonRetriableErr,
		},
		{
			name: "error - retriable but always fails",
			querier: &mocks.Querier{
				QueryRowFn: func(_ context.Context, _ []any, _ string, _ ...any) error {
					return retriableErr
				},
			},
			wantErr: retriableErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := &Querier{
				connBuilder: func(ctx context.Context) (postgres.Querier, error) {
					return tc.querier, nil
				},
				querier:         tc.querier,
				backoffProvider: newMockBackoffProvider(),
				logger:          loglib.NewNoopLogger(),
			}

			gotErr := q.QueryRow(context.Background(), []any{}, "SELECT 1")
			require.ErrorIs(t, gotErr, tc.wantErr)
		})
	}
}

func TestQuerier_Exec(t *testing.T) {
	t.Parallel()

	retriableErr := errors.New("connection failed")
	nonRetriableErr := context.Canceled

	testCommandTag := postgres.CommandTag{CommandTag: pgconn.NewCommandTag("UPDATE 1")}

	tests := []struct {
		name       string
		querier    postgres.Querier
		wantCmdTag postgres.CommandTag
		wantErr    error
	}{
		{
			name: "ok",
			querier: &mocks.Querier{
				ExecFn: func(_ context.Context, _ uint, _ string, _ ...any) (postgres.CommandTag, error) {
					return testCommandTag, nil
				},
			},
			wantCmdTag: testCommandTag,
			wantErr:    nil,
		},
		{
			name: "error - non-retriable",
			querier: &mocks.Querier{
				ExecFn: func(_ context.Context, _ uint, _ string, _ ...any) (postgres.CommandTag, error) {
					return postgres.CommandTag{}, nonRetriableErr
				},
			},
			wantCmdTag: postgres.CommandTag{},
			wantErr:    nonRetriableErr,
		},
		{
			name: "error - retriable but always fails",
			querier: &mocks.Querier{
				ExecFn: func(_ context.Context, _ uint, _ string, _ ...any) (postgres.CommandTag, error) {
					return postgres.CommandTag{}, retriableErr
				},
			},
			wantCmdTag: postgres.CommandTag{},
			wantErr:    retriableErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := &Querier{
				connBuilder: func(ctx context.Context) (postgres.Querier, error) {
					return tc.querier, nil
				},
				querier:         tc.querier,
				backoffProvider: newMockBackoffProvider(),
				logger:          loglib.NewNoopLogger(),
			}

			gotCmdTag, gotErr := q.Exec(context.Background(), "UPDATE test SET id = 1")
			require.ErrorIs(t, gotErr, tc.wantErr)
			require.Equal(t, tc.wantCmdTag, gotCmdTag)
		})
	}
}

func TestQuerier_ExecInTx(t *testing.T) {
	t.Parallel()

	retriableErr := errors.New("connection failed")
	nonRetriableErr := context.Canceled

	tests := []struct {
		name    string
		querier postgres.Querier
		wantErr error
	}{
		{
			name: "ok",
			querier: &mocks.Querier{
				ExecInTxFn: func(_ context.Context, _ func(tx postgres.Tx) error) error {
					return nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error - non-retriable",
			querier: &mocks.Querier{
				ExecInTxFn: func(_ context.Context, _ func(tx postgres.Tx) error) error {
					return nonRetriableErr
				},
			},
			wantErr: nonRetriableErr,
		},
		{
			name: "error - retriable but always fails",
			querier: &mocks.Querier{
				ExecInTxFn: func(_ context.Context, _ func(tx postgres.Tx) error) error {
					return retriableErr
				},
			},
			wantErr: retriableErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := &Querier{
				connBuilder: func(ctx context.Context) (postgres.Querier, error) {
					return tc.querier, nil
				},
				querier:         tc.querier,
				backoffProvider: newMockBackoffProvider(),
				logger:          loglib.NewNoopLogger(),
			}

			gotErr := q.ExecInTx(context.Background(), func(tx postgres.Tx) error {
				return nil
			})
			require.ErrorIs(t, gotErr, tc.wantErr)
		})
	}
}

func TestQuerier_ExecInTxWithOptions(t *testing.T) {
	t.Parallel()

	retriableErr := errors.New("connection failed")
	nonRetriableErr := context.Canceled

	tests := []struct {
		name    string
		querier postgres.Querier
		wantErr error
	}{
		{
			name: "ok",
			querier: &mocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, _ uint, _ func(tx postgres.Tx) error, _ postgres.TxOptions) error {
					return nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error - non-retriable",
			querier: &mocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, _ uint, _ func(tx postgres.Tx) error, _ postgres.TxOptions) error {
					return nonRetriableErr
				},
			},
			wantErr: nonRetriableErr,
		},
		{
			name: "error - retriable but always fails",
			querier: &mocks.Querier{
				ExecInTxWithOptionsFn: func(_ context.Context, _ uint, _ func(tx postgres.Tx) error, _ postgres.TxOptions) error {
					return retriableErr
				},
			},
			wantErr: retriableErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := &Querier{
				connBuilder: func(ctx context.Context) (postgres.Querier, error) {
					return tc.querier, nil
				},
				querier:         tc.querier,
				backoffProvider: newMockBackoffProvider(),
				logger:          loglib.NewNoopLogger(),
			}

			gotErr := q.ExecInTxWithOptions(context.Background(), func(tx postgres.Tx) error {
				return nil
			}, postgres.TxOptions{})
			require.ErrorIs(t, gotErr, tc.wantErr)
		})
	}
}

func TestQuerier_CopyFrom(t *testing.T) {
	t.Parallel()

	retriableErr := errors.New("connection failed")
	nonRetriableErr := context.Canceled

	tests := []struct {
		name      string
		querier   postgres.Querier
		wantCount int64
		wantErr   error
	}{
		{
			name: "ok",
			querier: &mocks.Querier{
				CopyFromFn: func(_ context.Context, _ string, _ []string, _ [][]any) (int64, error) {
					return 5, nil
				},
			},
			wantCount: 5,
			wantErr:   nil,
		},
		{
			name: "error - non-retriable",
			querier: &mocks.Querier{
				CopyFromFn: func(_ context.Context, _ string, _ []string, _ [][]any) (int64, error) {
					return 0, nonRetriableErr
				},
			},
			wantCount: 0,
			wantErr:   nonRetriableErr,
		},
		{
			name: "error - retriable but always fails",
			querier: &mocks.Querier{
				CopyFromFn: func(_ context.Context, _ string, _ []string, _ [][]any) (int64, error) {
					return 0, retriableErr
				},
			},
			wantCount: 0,
			wantErr:   retriableErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := &Querier{
				connBuilder: func(ctx context.Context) (postgres.Querier, error) {
					return tc.querier, nil
				},
				querier:         tc.querier,
				backoffProvider: newMockBackoffProvider(),
				logger:          loglib.NewNoopLogger(),
			}

			gotCount, gotErr := q.CopyFrom(context.Background(), "test_table", []string{"id", "name"}, [][]any{{1, "test"}})
			require.ErrorIs(t, gotErr, tc.wantErr)
			require.Equal(t, tc.wantCount, gotCount)
		})
	}
}

func TestQuerier_withRetry(t *testing.T) {
	t.Parallel()

	nonRetriableErr := context.Canceled
	retriableErr := errors.New("retriable error")
	connBuilderErr := errors.New("connection builder error")

	tests := []struct {
		name            string
		connBuilder     func(ctx context.Context) (postgres.Querier, error)
		backoffProvider backoff.Provider
		op              func(i uint) error

		wantErr error
	}{
		{
			name: "ok",
			op: func(_ uint) error {
				return nil
			},

			wantErr: nil,
		},
		{
			name: "non retriable error",
			op: func(_ uint) error {
				return nonRetriableErr
			},

			wantErr: nonRetriableErr,
		},
		{
			name: "retriable error and then succeeds",
			connBuilder: func(ctx context.Context) (postgres.Querier, error) {
				return &mocks.Querier{}, nil
			},
			backoffProvider: newMockBackoffProvider(),
			op: func(i uint) error {
				if i < 3 {
					return retriableErr
				}
				return nil
			},

			wantErr: nil,
		},
		{
			name: "retriable error but always fails",
			connBuilder: func(ctx context.Context) (postgres.Querier, error) {
				return &mocks.Querier{}, nil
			},
			backoffProvider: newMockBackoffProvider(),
			op: func(_ uint) error {
				return retriableErr
			},

			wantErr: retriableErr,
		},
		{
			name: "non-retriable error after some retriable errors",
			connBuilder: func(ctx context.Context) (postgres.Querier, error) {
				return &mocks.Querier{}, nil
			},
			backoffProvider: newMockBackoffProvider(),
			op: func(i uint) error {
				if i < 3 {
					return retriableErr
				}
				return nonRetriableErr
			},

			wantErr: nonRetriableErr,
		},
		{
			name: "error resetting connection after retriable errors",
			connBuilder: func(ctx context.Context) (postgres.Querier, error) {
				return nil, connBuilderErr
			},
			backoffProvider: newMockBackoffProvider(),
			op: func(i uint) error {
				return retriableErr
			},

			wantErr: connBuilderErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := &Querier{
				connBuilder:     tc.connBuilder,
				backoffProvider: tc.backoffProvider,
				logger:          loglib.NewNoopLogger(),
			}

			mockOp := mockOperation{
				op: tc.op,
			}

			gotErr := q.withRetry(context.Background(), mockOp.Do)
			require.ErrorIs(t, gotErr, tc.wantErr)
		})
	}
}

func TestQuerier_isRetriableError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		err             error
		wantIsRetriable bool
	}{
		{
			name:            "context canceled",
			err:             context.Canceled,
			wantIsRetriable: false,
		},
		{
			name:            "permission denied error",
			err:             &postgres.ErrPermissionDenied{},
			wantIsRetriable: false,
		},
		{
			name:            "constraint violation error",
			err:             &postgres.ErrConstraintViolation{},
			wantIsRetriable: false,
		},
		{
			name:            "generic error",
			err:             errors.New("generic error"),
			wantIsRetriable: true,
		},
		{
			name:            "wrapped context canceled",
			err:             fmt.Errorf("operation failed: %w", context.Canceled),
			wantIsRetriable: false,
		},
		{
			name:            "wrapped permission denied",
			err:             fmt.Errorf("operation failed: %w", &postgres.ErrPermissionDenied{}),
			wantIsRetriable: false,
		},
		{
			name:            "wrapped constraint violation",
			err:             fmt.Errorf("operation failed: %w", &postgres.ErrConstraintViolation{}),
			wantIsRetriable: false,
		},
		{
			name:            "wrapped generic error",
			err:             fmt.Errorf("operation failed: %w", errors.New("generic error")),
			wantIsRetriable: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			q := &Querier{
				logger: loglib.NewNoopLogger(),
			}

			got := q.isRetriableError(tc.err)
			require.Equal(t, tc.wantIsRetriable, got)
		})
	}
}

// mock backoff provider runs the operation for up to 3 times until it succeeds
// or returns error
func newMockBackoffProvider() backoff.Provider {
	return func(ctx context.Context) backoff.Backoff {
		return backoff.NewConstantBackoff(ctx, &backoff.ConstantConfig{
			Interval:   0,
			MaxRetries: 3,
		})
	}
}

type mockOperation struct {
	calls uint
	op    func(i uint) error
}

func (m *mockOperation) Do() error {
	m.calls++
	return m.op(m.calls)
}
