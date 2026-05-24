// SPDX-License-Identifier: Apache-2.0

package sanitizer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
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
						{Name: "id", Value: 1},
						{Name: "name", Value: "alice"},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "id", Value: 1},
				{Name: "name", Value: "alice"},
			},
		},
		{
			name: "strips null bytes from string values",
			event: &wal.Event{
				Data: &wal.Data{
					Action: "I",
					Columns: []wal.Column{
						{Name: "id", Value: 1},
						{Name: "name", Value: "ali\x00ce"},
						{Name: "bio", Value: "\x00hello\x00world\x00"},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "id", Value: 1},
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
						{Name: "id", Value: 42},
						{Name: "active", Value: true},
						{Name: "data", Value: nil},
					},
				},
			},
			wantColumns: []wal.Column{
				{Name: "id", Value: 42},
				{Name: "active", Value: true},
				{Name: "data", Value: nil},
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
					{Name: "id", Value: 42},
				},
			},
			want: "42",
		},
		{
			name: "composite identity",
			data: &wal.Data{
				Identity: []wal.Column{
					{Name: "tenant_id", Value: "abc"},
					{Name: "id", Value: 7},
				},
			},
			want: "tenant_id=abc,id=7",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, identityValues(tc.data))
		})
	}
}
