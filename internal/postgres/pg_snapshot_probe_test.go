// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	postgres "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
)

// snapshotMissingErr is what MapError produces for SQLSTATE 42704 on a SET
// TRANSACTION SNAPSHOT against a load-balanced source.
var snapshotMissingErr = &postgres.ErrRelationDoesNotExist{Details: `snapshot "00000D0E-0000001A-1" does not exist`}

func exporterMock(snapshotID string, queryErr error) *mocks.Querier {
	return &mocks.Querier{
		ExecInTxWithOptionsFn: func(ctx context.Context, _ uint, fn func(tx postgres.Tx) error, _ postgres.TxOptions) error {
			return fn(&mocks.Tx{
				QueryRowFn: func(_ context.Context, dest []any, _ string, _ ...any) error {
					if queryErr != nil {
						return queryErr
					}
					if ptr, ok := dest[0].(*string); ok {
						*ptr = snapshotID
					}
					return nil
				},
			})
		},
	}
}

// errWhen returns err when cond holds, else nil — used to pick a per-importer
// outcome by index.
func errWhen(cond bool, err error) error {
	if cond {
		return err
	}
	return nil
}

func importerMock(execErr error) *mocks.Querier {
	return &mocks.Querier{
		ExecInTxWithOptionsFn: func(ctx context.Context, _ uint, fn func(tx postgres.Tx) error, _ postgres.TxOptions) error {
			return fn(&mocks.Tx{
				ExecFn: func(_ context.Context, _ uint, _ string, _ ...any) (postgres.CommandTag, error) {
					return postgres.CommandTag{}, execErr
				},
			})
		},
	}
}

// dialer hands out the exporter on the first call and an importer built by
// importerFor on every subsequent (concurrent) call.
func dialer(exporter *mocks.Querier, importerFor func(importerIdx int) *mocks.Querier) func(context.Context) (postgres.Querier, error) {
	var calls int32
	return func(context.Context) (postgres.Querier, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return exporter, nil
		}
		return importerFor(int(n) - 2), nil
	}
}

func TestProbeExportedSnapshotVisibility(t *testing.T) {
	t.Parallel()

	otherErr := errors.New("boom")

	tests := []struct {
		name            string
		probes          int
		exporter        *mocks.Querier
		importerFor     func(int) *mocks.Querier
		dial            func(context.Context) (postgres.Querier, error)
		wantMissing     int
		wantErr         bool
		wantExportError bool
	}{
		{
			name:        "single instance - all importers succeed",
			probes:      4,
			exporter:    exporterMock("0000004D-00001943-1", nil),
			importerFor: func(int) *mocks.Querier { return importerMock(nil) },
		},
		{
			name:        "load balanced - all importers report snapshot missing",
			probes:      4,
			exporter:    exporterMock("0000004D-00001943-1", nil),
			importerFor: func(int) *mocks.Querier { return importerMock(snapshotMissingErr) },
			wantMissing: 4,
		},
		{
			name:     "load balanced - only some importers land elsewhere",
			probes:   4,
			exporter: exporterMock("0000004D-00001943-1", nil),
			importerFor: func(i int) *mocks.Querier {
				return importerMock(errWhen(i%2 == 0, snapshotMissingErr))
			},
			wantMissing: 2,
		},
		{
			name:        "non-snapshot error on one probe is ignored when another succeeds",
			probes:      4,
			exporter:    exporterMock("0000004D-00001943-1", nil),
			importerFor: func(i int) *mocks.Querier { return importerMock(errWhen(i == 0, otherErr)) },
		},
		{
			name:        "all probes fail for a non-snapshot reason - error",
			probes:      3,
			exporter:    exporterMock("0000004D-00001943-1", nil),
			importerFor: func(int) *mocks.Querier { return importerMock(otherErr) },
			wantErr:     true,
		},
		{
			name:            "exporter cannot export a snapshot - export failed error",
			probes:          3,
			exporter:        exporterMock("", otherErr),
			importerFor:     func(int) *mocks.Querier { return importerMock(nil) },
			wantErr:         true,
			wantExportError: true,
		},
		{
			name:            "malformed snapshot id - export failed error",
			probes:          3,
			exporter:        exporterMock("not a snapshot id", nil),
			importerFor:     func(int) *mocks.Querier { return importerMock(nil) },
			wantErr:         true,
			wantExportError: true,
		},
		{
			name:            "dial failure - export failed error",
			probes:          4,
			dial:            func(context.Context) (postgres.Querier, error) { return nil, errors.New("connection refused") },
			wantErr:         true,
			wantExportError: true,
		},
		{
			name:    "no probes requested - error",
			probes:  0,
			dial:    func(context.Context) (postgres.Querier, error) { return exporterMock("0000004D-00001943-1", nil), nil },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dial := tt.dial
			if dial == nil {
				dial = dialer(tt.exporter, tt.importerFor)
			}
			missing, err := postgres.ProbeExportedSnapshotVisibility(context.Background(), dial, tt.probes)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantExportError, errors.Is(err, postgres.ErrSnapshotExportFailed))
			require.Equal(t, tt.wantMissing, missing)
		})
	}
}

func TestIsExportedSnapshotMissing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		// SET TRANSACTION SNAPSHOT only produces 42704 for a missing snapshot, so
		// the mapped ErrRelationDoesNotExist is matched regardless of message
		// locale.
		{name: "mapped ErrRelationDoesNotExist (localized message)", err: &postgres.ErrRelationDoesNotExist{Details: `no existe el snapshot «00000D0E-0000001A-1»`}, want: true},
		{name: "mapped ErrRelationDoesNotExist (english)", err: snapshotMissingErr, want: true},
		{name: "raw english text not mapped", err: errors.New(`ERROR: snapshot "00000D0E-0000001A-1" does not exist (SQLSTATE 42704)`), want: true},
		{name: "different error type", err: &postgres.ErrPermissionDenied{Details: "permission denied"}, want: false},
		{name: "unrelated error", err: errors.New("connection refused"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, postgres.IsExportedSnapshotMissing(tt.err))
		})
	}
}
