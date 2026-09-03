// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/internal/progress"
	progressmocks "github.com/xataio/pgstream/internal/progress/mocks"
	synclib "github.com/xataio/pgstream/internal/sync"
	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
)

func TestBuildCopyToSQL(t *testing.T) {
	t.Parallel()

	// the strategy's query is wrapped verbatim, whatever selected the chunk
	require.Equal(t,
		`COPY (SELECT "id" FROM ONLY "public"."users" WHERE ctid BETWEEN '(0,0)' AND '(10,0)') TO STDOUT WITH (FORMAT binary)`,
		buildCopyToSQL(`SELECT "id" FROM ONLY "public"."users" WHERE ctid BETWEEN '(0,0)' AND '(10,0)'`))
	require.Equal(t,
		`COPY (SELECT "id" FROM ONLY "public"."users" WHERE "id" > 42 ORDER BY "id" LIMIT 1000) TO STDOUT WITH (FORMAT binary)`,
		buildCopyToSQL(`SELECT "id" FROM ONLY "public"."users" WHERE "id" > 42 ORDER BY "id" LIMIT 1000`))
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

func TestCopyPassthroughMover_copyRange(t *testing.T) {
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

		wantRows    uint
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

			rows, err := s.copyChunkInTx(t.Context(), tc.sourceTx, testTable, "SELECT id FROM t")
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

func TestCopyPassthroughMover_prepareTargetTx(t *testing.T) {
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

			s := &copyPassthroughMover{cfg: &CopyPassthroughConfig{DisableTriggers: tc.disableTriggers}}
			require.NoError(t, s.prepareTargetTx(t.Context(), tx))
			require.Equal(t, tc.wantQueries, queries)
		})
	}
}

func TestWithoutColumns(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"id", "name"},
		withoutColumns([]string{"id", "name"}, nil))
	require.Equal(t, []string{"id", "name"},
		withoutColumns([]string{"id", "name", "username", "slug"}, []string{"username", "slug"}))
	require.Equal(t, []string{"id"},
		withoutColumns([]string{"id"}, []string{"username"}))
	// every column generated: nothing left for COPY to carry
	require.Empty(t, withoutColumns([]string{"slug"}, []string{"slug"}))
}

func TestCopyPassthroughMover_snapshotRange_fallback(t *testing.T) {
	t.Parallel()

	copyable := &table{schema: "public", name: "users", columns: []string{"id"}}
	// prepareTable marks the table; its columns stay, so the decoding path
	// still selects them instead of widening to SELECT *
	allGenerated := &table{schema: "public", name: "users", columns: []string{"slug"}, decodeOnly: true}

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
			_, err := s.move(t.Context(), run, tc.table, "SELECT id FROM t")
			require.NoError(t, err)

			require.Equal(t, tc.wantCopied, copied)
			require.Equal(t, tc.wantFellBack, fallback.called)
		})
	}
}

func newTestCopyPassthrough(targetConn pglib.Querier, fallback chunkMover) *copyPassthroughMover {
	return &copyPassthroughMover{
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

func (s *stubSnapshotter) move(context.Context, runInTx, *table, string) (uint, error) {
	s.called = true
	return 0, nil
}

// a retry must re-read the source, not resume a drained pipe
func TestCopyPassthroughMover_snapshotRange_retries(t *testing.T) {
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

			_, err := s.move(t.Context(), run, testTable, "SELECT id FROM t")
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

func TestCopyPassthroughMover_prepareTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		columns   []string
		generated []string

		wantColumns    []string
		wantDecodeOnly bool
	}{
		{
			name:        "nothing generated leaves the column list alone",
			columns:     []string{"id", "name"},
			wantColumns: []string{"id", "name"},
		},
		{
			name:        "a generated column is pruned from what the strategy selects",
			columns:     []string{"id", "name", "slug"},
			generated:   []string{"slug"},
			wantColumns: []string{"id", "name"},
		},
		{
			// emptying the list would widen the reader's query to SELECT *, and
			// the decoding path would then hand the writer the generated
			// columns the target rejects
			name:           "an all generated table keeps its columns and is decoded",
			columns:        []string{"slug"},
			generated:      []string{"slug"},
			wantColumns:    []string{"slug"},
			wantDecodeOnly: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestCopyPassthrough(&mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (pglib.Rows, error) {
					return generatedColumnRows(t, tc.generated), nil
				},
			}, &stubSnapshotter{})

			tbl := &table{schema: "public", name: "users", columns: append([]string(nil), tc.columns...)}
			require.NoError(t, s.prepareTable(t.Context(), tbl))

			require.Equal(t, tc.wantColumns, tbl.columns)
			require.Equal(t, tc.wantDecodeOnly, tbl.decodeOnly)
		})
	}
}

// the target transaction is nested in the source one and commits first, so a
// source failure landing after it must fail the range instead of copying it
// again: nothing downstream can tell the duplicate rows apart
func TestCopyPassthroughMover_move_sourceFailsAfterTargetCommit(t *testing.T) {
	t.Parallel()

	// retriable on its own, so only the commit ordering keeps it from retrying
	errSourceCommit := errors.New("connection reset by peer")
	testTable := &table{schema: "public", name: "users", columns: []string{"id"}}

	attempts := 0
	run := func(ctx context.Context, fn func(tx pglib.Tx) error) error {
		attempts++
		if err := fn(&mocks.Tx{
			CopyToWriterFn: func(_ context.Context, w io.Writer, _ string) (int64, error) {
				_, err := io.WriteString(w, "1\n")
				return 1, err
			},
		}); err != nil {
			return err
		}
		// the source transaction commits once the target's already has
		return errSourceCommit
	}

	s := newTestCopyPassthrough(newStubTargetConn(1), nil)
	s.backoffProvider = backoff.NewProvider(&backoff.Config{
		Constant: &backoff.ConstantConfig{Interval: time.Millisecond, MaxRetries: 3},
	})

	_, err := s.move(t.Context(), run, testTable, "SELECT id FROM t")
	require.ErrorIs(t, err, errChunkAlreadyCommitted)
	require.ErrorIs(t, err, errSourceCommit)
	require.Equal(t, 1, attempts)
}

// the sink reports the bytes the decoding path moves; the passthrough has no
// sink, so without its own report the schema's bar never leaves zero
func TestCopyPassthroughMover_move_progress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table *table

		wantAdvanced []int64
	}{
		{
			name:         "the copied bytes advance the schema bar",
			table:        &table{schema: "public", name: "users", columns: []string{"id"}, rowSize: 10},
			wantAdvanced: []int64{30},
		},
		{
			name:  "a delegated table is reported by the sink, not here",
			table: &table{schema: "public", name: "users", columns: []string{"slug"}, rowSize: 10, decodeOnly: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var advanced []int64
			tracker := progressTracker{enabled: true, bars: synclib.NewMap[string, progress.Bar]()}
			tracker.set(tc.table.schema, &progressmocks.Bar{
				Add64Fn: func(n int64) error {
					advanced = append(advanced, n)
					return nil
				},
			})

			s := newTestCopyPassthrough(newStubTargetConn(3), &stubSnapshotter{})
			s.progress = tracker

			run := func(ctx context.Context, fn func(tx pglib.Tx) error) error {
				return fn(&mocks.Tx{
					CopyToWriterFn: func(_ context.Context, w io.Writer, _ string) (int64, error) {
						_, err := io.WriteString(w, "1\n2\n3\n")
						return 3, err
					},
				})
			}

			_, err := s.move(t.Context(), run, tc.table, "SELECT id FROM t")
			require.NoError(t, err)
			require.Equal(t, tc.wantAdvanced, advanced)
		})
	}
}

func newStubTargetConn(rows int64) *mocks.Querier {
	return &mocks.Querier{
		ExecInTxFn: func(ctx context.Context, fn func(tx pglib.Tx) error) error {
			return fn(&mocks.Tx{
				ExecFn: func(context.Context, uint, string, ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, nil
				},
				CopyFromReaderFn: func(_ context.Context, r io.Reader, _ string) (int64, error) {
					if _, err := io.ReadAll(r); err != nil {
						return -1, err
					}
					return rows, nil
				},
			})
		},
	}
}

func generatedColumnRows(t *testing.T, columns []string) *mocks.Rows {
	t.Helper()
	return &mocks.Rows{
		NextFn: func(i uint) bool { return i <= uint(len(columns)) },
		ScanFn: func(i uint, dest ...any) error {
			require.Len(t, dest, 1)
			column, ok := dest[0].(*string)
			require.True(t, ok, fmt.Sprintf("column, expected *string, got %T", dest[0]))
			*column = columns[i-1]
			return nil
		},
		ErrFn:   func() error { return nil },
		CloseFn: func() {},
	}
}
