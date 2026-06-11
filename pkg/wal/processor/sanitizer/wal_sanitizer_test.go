// SPDX-License-Identifier: Apache-2.0

package sanitizer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

type mockProcessor struct {
	events []*wal.Event
	err    error
}

func (m *mockProcessor) ProcessWALEvent(_ context.Context, event *wal.Event) error {
	m.events = append(m.events, event)
	return m.err
}

func (m *mockProcessor) Close() error { return nil }
func (m *mockProcessor) Name() string { return "mock" }

func TestSanitizer_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		event       *wal.Event
		wantColumns []wal.Column
	}{
		{
			name: "no null bytes - unchanged",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Columns: []wal.Column{
						{Name: "id", Value: int32(1)},
						{Name: "name", Value: "alice"},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "id", Value: int32(1)},
				{Name: "name", Value: "alice"},
			},
		},
		{
			name: "strips null bytes from string values",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Schema: "public",
					Table:  "users",
					Columns: []wal.Column{
						{Name: "id", Value: int32(1)},
						{Name: "name", Value: "ali\x00ce"},
						{Name: "bio", Value: "\x00hello\x00world\x00"},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "id", Value: int32(1)},
				{Name: "name", Value: "alice"},
				{Name: "bio", Value: "helloworld"},
			},
		},
		{
			name: "nil data - no panic",
			event: &wal.Event{
				Data: nil,
			},
			wantColumns: nil,
		},
		{
			name: "non-string values unchanged",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Columns: []wal.Column{
						{Name: "id", Value: int32(42)},
						{Name: "active", Value: true},
						{Name: "data", Value: nil},
						{Name: "score", Value: float64(3.14)},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "id", Value: int32(42)},
				{Name: "active", Value: true},
				{Name: "data", Value: nil},
				{Name: "score", Value: float64(3.14)},
			},
		},
		{
			name: "empty string unchanged",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Columns: []wal.Column{
						{Name: "name", Value: ""},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "name", Value: ""},
			},
		},
		{
			name: "multiple string columns with null bytes",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "U",
					Schema: "test",
					Table:  "items",
					Columns: []wal.Column{
						{Name: "title", Value: "hello\x00world"},
						{Name: "description", Value: "normal text"},
						{Name: "tags", Value: "\x00a\x00b\x00"},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "title", Value: "helloworld"},
				{Name: "description", Value: "normal text"},
				{Name: "tags", Value: "ab"},
			},
		},
		{
			name: "string with null byte at start only",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Columns: []wal.Column{
						{Name: "data", Value: "\x00important"},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "data", Value: "important"},
			},
		},
		{
			name: "string with only null bytes becomes empty",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Columns: []wal.Column{
						{Name: "junk", Value: "\x00\x00\x00"},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "junk", Value: ""},
			},
		},
		{
			name: "event data has nil columns",
			event: &wal.Event{
				Data: &wal.Data{
					Action:  "I",
					Schema:  "public",
					Table:   "test",
					Columns: nil,
				},
			},
			wantColumns: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mock := &mockProcessor{}
			s := New(mock, WithLogger(loglib.NewNoopLogger()))

			err := s.ProcessWALEvent(context.Background(), tc.event)
			require.NoError(t, err)
			require.Len(t, mock.events, 1)
			if tc.event.Data != nil {
				require.Equal(t, tc.wantColumns, mock.events[0].Data.Columns)
			}
		})
	}
}

func TestSanitizer_ProcessWALEvent_Delegation(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	s := New(mock, WithLogger(loglib.NewNoopLogger()))

	event := &wal.Event{
		CommitPosition: "0/12345",
		Data: &wal.Data{
			Action: "I",
			Schema: "public",
			Table:  "users",
			Columns: []wal.Column{
				{Name: "id", Value: int32(1)},
				{Name: "name", Value: "test"},
			},
		},
	}

	err := s.ProcessWALEvent(context.Background(), event)
	require.NoError(t, err)
	require.Len(t, mock.events, 1)
	require.Equal(t, event, mock.events[0])
}

func TestSanitizer_Close(t *testing.T) {
	t.Parallel()

	closed := false
	s := New(&mockProcessorWithClose{closed: &closed})

	require.NoError(t, s.Close())
	require.True(t, closed)
}

func TestSanitizer_Name(t *testing.T) {
	t.Parallel()

	mock := &mockProcessor{}
	s := New(mock)
	require.Equal(t, "mock", s.Name())
}

type mockProcessorWithClose struct {
	processor.Processor
	closed *bool
}

func (m *mockProcessorWithClose) Close() error {
	*m.closed = true
	return nil
}

func (m *mockProcessorWithClose) Name() string {
	return "mock"
}

func (m *mockProcessorWithClose) ProcessWALEvent(_ context.Context, _ *wal.Event) error {
	return nil
}

func TestIdentityValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data *wal.Data
		want string
	}{
		{
			name: "no identity columns",
			data: &wal.Data{},
			want: "",
		},
		{
			name: "single identity column",
			data: &wal.Data{
				Identity: []wal.Column{
					{Name: "id", Value: int32(42)},
				},
			},
			want: "42",
		},
		{
			name: "composite identity",
			data: &wal.Data{
				Identity: []wal.Column{
					{Name: "tenant_id", Value: "abc"},
					{Name: "id", Value: int32(7)},
				},
			},
			want: "tenant_id=abc,id=7",
		},
		{
			name: "single identity with non-string value",
			data: &wal.Data{
				Identity: []wal.Column{
					{Name: "id", Value: int64(100)},
				},
			},
			want: "100",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, identityValues(tc.data))
		})
	}
}
