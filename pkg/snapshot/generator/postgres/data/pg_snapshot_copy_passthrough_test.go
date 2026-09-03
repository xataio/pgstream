// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	synclib "github.com/xataio/pgstream/internal/sync"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
)

func TestBuildCopyToSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table *table

		want string
	}{
		{
			name:  "explicit columns",
			table: &table{schema: "public", name: "users", columns: []string{"id", "name"}},
			want:  `COPY (SELECT "id", "name" FROM ONLY "public"."users" WHERE ctid BETWEEN '(0,0)' AND '(10,0)') TO STDOUT WITH (FORMAT binary)`,
		},
		{
			name:  "no columns falls back to star, as the decoding path does",
			table: &table{schema: "public", name: "users"},
			want:  `COPY (SELECT * FROM ONLY "public"."users" WHERE ctid BETWEEN '(0,0)' AND '(10,0)') TO STDOUT WITH (FORMAT binary)`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, buildCopyToSQL(tc.table, pageRange{start: 0, end: 10}))
		})
	}
}

func TestBuildCopyFromSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table *table

		want string
	}{
		{
			name:  "columns are named so target column order is irrelevant",
			table: &table{schema: "public", name: "users", columns: []string{"id", "name"}},
			want:  `COPY "public"."users" ("id", "name") FROM STDIN WITH (FORMAT binary)`,
		},
		{
			name:  "no columns omits the column list",
			table: &table{schema: "public", name: "users"},
			want:  `COPY "public"."users" FROM STDIN WITH (FORMAT binary)`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, buildCopyFromSQL(tc.table))
		})
	}
}

func TestCopyPassthroughSnapshotter_copyRange(t *testing.T) {
	t.Parallel()

	errSource := errors.New("source copy failed")
	errTarget := errors.New("target copy failed")
	testTable := &table{schema: "public", name: "users", columns: []string{"id"}}

	newSourceTx := func(payload string, rows int64, sourceErr error) *mocks.Tx {
		return &mocks.Tx{
			CopyToWriterFn: func(_ context.Context, w io.Writer, _ string) (int64, error) {
				if sourceErr != nil {
					return -1, sourceErr
				}
				if _, err := io.WriteString(w, payload); err != nil {
					return -1, err
				}
				return rows, nil
			},
		}
	}

	newTargetConn := func(rows int64, targetErr error, got *string) *mocks.Querier {
		return &mocks.Querier{
			ExecInTxFn: func(ctx context.Context, fn func(tx pglib.Tx) error) error {
				return fn(&mocks.Tx{
					ExecFn: func(context.Context, uint, string, ...any) (pglib.CommandTag, error) {
						return pglib.CommandTag{}, nil
					},
					CopyFromReaderFn: func(_ context.Context, r io.Reader, _ string) (int64, error) {
						if targetErr != nil {
							return -1, targetErr
						}
						b, err := io.ReadAll(r)
						if err != nil {
							return -1, err
						}
						*got = string(b)
						return rows, nil
					},
				})
			},
		}
	}

	tests := []struct {
		name      string
		sourceTx  *mocks.Tx
		targetRow int64
		targetErr error

		wantRows    int64
		wantPayload string
		wantErr     error
	}{
		{
			name:        "rows stream through unchanged",
			sourceTx:    newSourceTx("1\n2\n3\n", 3, nil),
			targetRow:   3,
			wantRows:    3,
			wantPayload: "1\n2\n3\n",
		},
		{
			name:      "row count mismatch is reported",
			sourceTx:  newSourceTx("1\n2\n3\n", 3, nil),
			targetRow: 2,
			wantErr:   errUnexpectedCopiedRows,
		},
		{
			name:      "source failure surfaces",
			sourceTx:  newSourceTx("", 0, errSource),
			targetRow: 0,
			wantErr:   errSource,
		},
		{
			name:      "target failure surfaces and does not block the source",
			sourceTx:  newSourceTx("1\n2\n3\n", 3, nil),
			targetErr: errTarget,
			wantErr:   errTarget,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got string
			s := newTestCopyPassthrough(newTargetConn(tc.targetRow, tc.targetErr, &got), nil)

			rows, err := s.copyRangeInTx(t.Context(), tc.sourceTx, testTable, pageRange{start: 0, end: 10})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantRows, rows)
			require.Equal(t, tc.wantPayload, got)
		})
	}
}

func TestCopyPassthroughSnapshotter_prepareTargetTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		disableTriggers bool

		wantQueries []string
	}{
		{
			name:            "triggers left alone by default",
			disableTriggers: false,
			wantQueries:     []string{targetLockTimeout},
		},
		{
			name:            "triggers suppressed for the copy",
			disableTriggers: true,
			wantQueries: []string{
				targetLockTimeout,
				"SET LOCAL session_replication_role = replica",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var queries []string
			tx := &mocks.Tx{
				ExecFn: func(_ context.Context, _ uint, query string, _ ...any) (pglib.CommandTag, error) {
					queries = append(queries, query)
					return pglib.CommandTag{}, nil
				},
			}

			s := &copyPassthroughSnapshotter{cfg: &CopyPassthroughConfig{DisableTriggers: tc.disableTriggers}}
			require.NoError(t, s.prepareTargetTx(t.Context(), tx))
			require.Equal(t, tc.wantQueries, queries)
		})
	}
}

func TestTable_withoutGeneratedColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table *table

		wantColumns []string
	}{
		{
			name:        "no generated columns leaves the list alone",
			table:       &table{columns: []string{"id", "name"}},
			wantColumns: []string{"id", "name"},
		},
		{
			name: "generated columns are dropped",
			table: &table{
				columns:          []string{"id", "name", "username", "slug"},
				generatedColumns: []string{"username", "slug"},
			},
			wantColumns: []string{"id", "name"},
		},
		{
			name: "a generated column that is not selected is ignored",
			table: &table{
				columns:          []string{"id"},
				generatedColumns: []string{"username"},
			},
			wantColumns: []string{"id"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			original := slices.Clone(tc.table.columns)
			got := tc.table.withoutGeneratedColumns()

			require.Equal(t, tc.wantColumns, got.columns)
			require.Equal(t, original, tc.table.columns, "the original table must not be modified")
		})
	}
}

func TestBuildCopySQL_excludesGeneratedColumns(t *testing.T) {
	t.Parallel()

	testTable := (&table{
		schema:           "public",
		name:             "users",
		columns:          []string{"id", "name", "username"},
		generatedColumns: []string{"username"},
	}).withoutGeneratedColumns()

	require.Equal(t,
		`COPY (SELECT "id", "name" FROM ONLY "public"."users" WHERE ctid BETWEEN '(0,0)' AND '(10,0)') TO STDOUT WITH (FORMAT binary)`,
		buildCopyToSQL(testTable, pageRange{start: 0, end: 10}))
	require.Equal(t,
		`COPY "public"."users" ("id", "name") FROM STDIN WITH (FORMAT binary)`,
		buildCopyFromSQL(testTable))
}

func TestTable_hasCopyableColumns(t *testing.T) {
	t.Parallel()

	require.True(t, (&table{columns: []string{"id"}}).hasCopyableColumns())
	require.True(t, (&table{
		columns:          []string{"id", "slug"},
		generatedColumns: []string{"slug"},
	}).hasCopyableColumns())

	// no COPY carries all-generated rows
	require.False(t, (&table{
		columns:          []string{"slug"},
		generatedColumns: []string{"slug"},
	}).hasCopyableColumns())
	// a star select the target cannot receive
	require.False(t, (&table{generatedColumns: []string{"slug"}}).hasCopyableColumns())
}

func TestCopyPassthroughSnapshotter_snapshotRange_fallback(t *testing.T) {
	t.Parallel()

	copyable := &table{schema: "public", name: "users", columns: []string{"id"}}
	allGenerated := &table{
		schema:           "public",
		name:             "users",
		columns:          []string{"slug"},
		generatedColumns: []string{"slug"},
	}

	tests := []struct {
		name  string
		table *table

		wantCopied   bool
		wantFellBack bool
	}{
		{
			name:       "copies the rows COPY can carry",
			table:      copyable,
			wantCopied: true,
		},
		{
			name:         "delegates a table COPY cannot carry",
			table:        allGenerated,
			wantFellBack: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var copied bool
			sourceTx := &mocks.Tx{
				CopyToWriterFn: func(_ context.Context, w io.Writer, _ string) (int64, error) {
					copied = true
					_, err := io.WriteString(w, "1\n")
					return 1, err
				},
			}
			targetConn := &mocks.Querier{
				ExecInTxFn: func(ctx context.Context, fn func(tx pglib.Tx) error) error {
					return fn(&mocks.Tx{
						ExecFn: func(context.Context, uint, string, ...any) (pglib.CommandTag, error) {
							return pglib.CommandTag{}, nil
						},
						CopyFromReaderFn: func(_ context.Context, r io.Reader, _ string) (int64, error) {
							if _, err := io.ReadAll(r); err != nil {
								return -1, err
							}
							return 1, nil
						},
					})
				},
			}

			fallback := &stubSnapshotter{}
			s := newTestCopyPassthrough(targetConn, fallback)

			run := func(ctx context.Context, fn func(tx pglib.Tx) error) error { return fn(sourceTx) }
			_, err := s.snapshotRange(t.Context(), run, tc.table, pageRange{start: 0, end: 1})
			require.NoError(t, err)

			require.Equal(t, tc.wantCopied, copied)
			require.Equal(t, tc.wantFellBack, fallback.called)
		})
	}
}

func newTestCopyPassthrough(targetConn pglib.Querier, fallback rangeSnapshotter) *copyPassthroughSnapshotter {
	return &copyPassthroughSnapshotter{
		cfg:        &CopyPassthroughConfig{},
		logger:     loglib.NewNoopLogger(),
		targetConn: targetConn,
		budget:     synclib.NewWeightedSemaphore(1),
		fallback:   fallback,
	}
}

type stubSnapshotter struct{ called bool }

func (s *stubSnapshotter) prepareTable(context.Context, *table) error { return nil }
func (s *stubSnapshotter) close(context.Context) error                { return nil }

func (s *stubSnapshotter) snapshotRange(context.Context, runInSnapshotTx, *table, pageRange) (int64, error) {
	s.called = true
	return 0, nil
}

// a retry must re-read the source, not resume a drained pipe
func TestCopyPassthroughSnapshotter_snapshotRange_retries(t *testing.T) {
	t.Parallel()

	testTable := &table{schema: "public", name: "users", columns: []string{"id"}}
	errRetriable := errors.New("connection reset by peer")

	tests := []struct {
		name      string
		targetErr func(attempt int) error
		targetRow func(attempt int) int64

		wantAttempts int
		wantErr      error
	}{
		{
			name:         "a retriable failure copies the range again",
			targetErr:    func(attempt int) error { return map[bool]error{true: errRetriable, false: nil}[attempt == 1] },
			targetRow:    func(int) int64 { return 3 },
			wantAttempts: 2,
		},
		{
			name:         "a row count mismatch is not retried",
			targetErr:    func(int) error { return nil },
			targetRow:    func(int) int64 { return 2 },
			wantAttempts: 1,
			wantErr:      errUnexpectedCopiedRows,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var attempts int
			var payloads []string
			run := func(ctx context.Context, fn func(tx pglib.Tx) error) error {
				attempts++
				return fn(&mocks.Tx{
					CopyToWriterFn: func(_ context.Context, w io.Writer, _ string) (int64, error) {
						_, err := io.WriteString(w, "1\n2\n3\n")
						return 3, err
					},
				})
			}

			s := newTestCopyPassthrough(&mocks.Querier{
				ExecInTxFn: func(ctx context.Context, fn func(tx pglib.Tx) error) error {
					return fn(&mocks.Tx{
						ExecFn: func(context.Context, uint, string, ...any) (pglib.CommandTag, error) {
							return pglib.CommandTag{}, nil
						},
						CopyFromReaderFn: func(_ context.Context, r io.Reader, _ string) (int64, error) {
							b, readErr := io.ReadAll(r)
							if readErr != nil {
								return -1, readErr
							}
							payloads = append(payloads, string(b))
							if err := tc.targetErr(attempts); err != nil {
								return -1, err
							}
							return tc.targetRow(attempts), nil
						},
					})
				},
			}, nil)
			s.backoffProvider = backoff.NewProvider(&backoff.Config{
				Constant: &backoff.ConstantConfig{Interval: time.Millisecond, MaxRetries: 3},
			})

			_, err := s.snapshotRange(t.Context(), run, testTable, pageRange{start: 0, end: 10})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.wantAttempts, attempts)
			// every attempt read the whole range, never a partial pipe
			for _, payload := range payloads {
				require.Equal(t, "1\n2\n3\n", payload)
			}
		})
	}
}
