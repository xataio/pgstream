// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

func TestStoreCache_Fetch(t *testing.T) {
	t.Parallel()

	const testSchema = "test-schema"
	const testAckedOnly = true
	testLogEntry := &LogEntry{
		ID:         xid.New(),
		SchemaName: testSchema,
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name  string
		store Store
		cache map[string]*LogEntry

		wantLogEntry *LogEntry
		wantErr      error
	}{
		{
			name: "ok - cache miss",
			store: &mockStore{
				fetchFn: func(schemaName string, ackedOnly bool) (*LogEntry, error) {
					require.Equal(t, schemaName, testSchema)
					require.Equal(t, ackedOnly, testAckedOnly)
					return testLogEntry, nil
				},
			},
			cache: map[string]*LogEntry{},

			wantLogEntry: testLogEntry,
			wantErr:      nil,
		},
		{
			name: "ok - cache hit",
			store: &mockStore{
				fetchFn: func(schemaName string, ackedOnly bool) (*LogEntry, error) {
					return nil, errors.New("fetchFn: should not be called")
				},
			},
			cache: map[string]*LogEntry{
				testSchema: testLogEntry,
			},

			wantLogEntry: testLogEntry,
			wantErr:      nil,
		},
		{
			name: "error - fetching schema",
			store: &mockStore{
				fetchFn: func(schemaName string, ackedOnly bool) (*LogEntry, error) {
					return nil, errTest
				},
			},
			cache: map[string]*LogEntry{},

			wantLogEntry: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreCache(tc.store)
			if tc.cache != nil {
				s.cache = tc.cache
			}

			le, err := s.Fetch(context.Background(), testSchema, testAckedOnly)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, le, tc.wantLogEntry)
		})
	}
}

func TestStoreCache_Ack(t *testing.T) {
	t.Parallel()

	const testSchema = "test-schema"
	testLogEntry := &LogEntry{
		ID:         xid.New(),
		SchemaName: testSchema,
		Version:    1,
	}

	updatedTestLogEntry := &LogEntry{
		ID:         xid.New(),
		SchemaName: testSchema,
		Version:    2,
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name  string
		store Store
		cache map[string]*LogEntry

		wantCache map[string]*LogEntry
		wantErr   error
	}{
		{
			name: "ok",
			store: &mockStore{
				ackFn: func(le *LogEntry) error {
					require.Equal(t, updatedTestLogEntry, le)
					return nil
				},
			},
			cache: map[string]*LogEntry{
				testSchema: testLogEntry,
			},

			wantCache: map[string]*LogEntry{
				testSchema: updatedTestLogEntry,
			},
			wantErr: nil,
		},
		{
			name: "error - acking",
			store: &mockStore{
				ackFn: func(le *LogEntry) error {
					require.Equal(t, updatedTestLogEntry, le)
					return errTest
				},
			},
			cache: map[string]*LogEntry{
				testSchema: testLogEntry,
			},

			wantCache: map[string]*LogEntry{
				testSchema: updatedTestLogEntry,
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreCache(tc.store)
			if tc.cache != nil {
				s.cache = tc.cache
			}

			err := s.Ack(context.Background(), updatedTestLogEntry)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, s.cache, tc.wantCache)
		})
	}
}

func TestStoreCache_Insert(t *testing.T) {
	t.Parallel()

	const testSchema = "test-schema"
	testLogEntry := &LogEntry{
		ID:         xid.New(),
		SchemaName: testSchema,
		Version:    1,
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name  string
		store Store
		cache map[string]*LogEntry

		wantCache map[string]*LogEntry
		wantErr   error
	}{
		{
			name: "ok",
			store: &mockStore{
				insertFn: func(schemaName string) (*LogEntry, error) {
					require.Equal(t, testSchema, schemaName)
					return testLogEntry, nil
				},
			},
			wantCache: map[string]*LogEntry{},
			wantErr:   nil,
		},
		{
			name: "error - inserting",
			store: &mockStore{
				insertFn: func(schemaName string) (*LogEntry, error) {
					return nil, errTest
				},
			},
			wantCache: map[string]*LogEntry{},
			wantErr:   errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStoreCache(tc.store)
			if tc.cache != nil {
				s.cache = tc.cache
			}

			_, err := s.Insert(context.Background(), testSchema)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, s.cache, tc.wantCache)
		})
	}
}

type mockStore struct {
	insertFn func(schemaName string) (*LogEntry, error)
	fetchFn  func(schemaName string, ackedOnly bool) (*LogEntry, error)
	ackFn    func(le *LogEntry) error
}

func (m *mockStore) Insert(_ context.Context, schemaName string) (*LogEntry, error) {
	return m.insertFn(schemaName)
}

func (m *mockStore) Fetch(_ context.Context, schemaName string, ackedOnly bool) (*LogEntry, error) {
	return m.fetchFn(schemaName, ackedOnly)
}

func (m *mockStore) Ack(_ context.Context, le *LogEntry) error {
	return m.ackFn(le)
}

func (m *mockStore) Close() error {
	return nil
}
