// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
)

type mockPgDump struct {
	dumpFn    func(context.Context, uint, pglib.PGDumpOptions) ([]byte, error)
	dumpCalls uint
}

func newMockPgdump(dumpFn func(context.Context, uint, pglib.PGDumpOptions) ([]byte, error)) pglib.PGDumpFn {
	m := &mockPgDump{
		dumpFn: dumpFn,
	}
	return m.dump
}

func (m *mockPgDump) dump(ctx context.Context, po pglib.PGDumpOptions) ([]byte, error) {
	m.dumpCalls++
	return m.dumpFn(ctx, m.dumpCalls, po)
}

type mockPgDumpAll struct {
	dumpFn    func(context.Context, uint, pglib.PGDumpAllOptions) ([]byte, error)
	dumpCalls uint
}

func newMockPgdumpall(dumpFn func(context.Context, uint, pglib.PGDumpAllOptions) ([]byte, error)) pglib.PGDumpAllFn {
	m := &mockPgDumpAll{
		dumpFn: dumpFn,
	}
	return m.dump
}

func (m *mockPgDumpAll) dump(ctx context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
	m.dumpCalls++
	return m.dumpFn(ctx, m.dumpCalls, po)
}

type mockPgRestore struct {
	restoreFn    func(context.Context, uint, pglib.PGRestoreOptions, []byte) (string, error)
	restoreCalls uint
}

func newMockPgrestore(restoreFn func(context.Context, uint, pglib.PGRestoreOptions, []byte) (string, error)) pglib.PGRestoreFn {
	m := &mockPgRestore{
		restoreFn: restoreFn,
	}
	return m.restore
}

func (m *mockPgRestore) restore(ctx context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
	m.restoreCalls++
	return m.restoreFn(ctx, m.restoreCalls, po, dump)
}

type mockSnapshotTracker struct {
	trackIndexesCreationFn func(context.Context)
	closeFn                func() error
}

func (m *mockSnapshotTracker) trackIndexesCreation(ctx context.Context) {
	if m.trackIndexesCreationFn != nil {
		m.trackIndexesCreationFn(ctx)
	}
}

func (m *mockSnapshotTracker) close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

// Helper function to create a mock querier that returns ErrNoRows (table not found)
func newNoTableQuerier() *pgmocks.Querier {
	return &pgmocks.Querier{
		QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
			return pglib.ErrNoRows
		},
	}
}
