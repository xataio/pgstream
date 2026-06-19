// SPDX-License-Identifier: Apache-2.0

package stdout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestWriter_ProcessWALEvent(t *testing.T) {
	t.Parallel()

	dataEvent := &wal.Event{
		Data: &wal.Data{
			Action: "I",
			Schema: "public",
			Table:  "users",
			Columns: []wal.Column{
				{Name: "id", Type: "int4", Value: int64(42)},
			},
		},
		CommitPosition: wal.CommitPosition("0/16BCAF8"),
	}
	keepAliveEvent := &wal.Event{
		CommitPosition: wal.CommitPosition("0/16BCB00"),
	}

	t.Run("writes NDJSON for data event", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		w := NewWriter(WithWriter(&buf))

		err := w.ProcessWALEvent(context.Background(), dataEvent)
		require.NoError(t, err)

		line := buf.String()
		require.True(t, strings.HasSuffix(line, "\n"), "expected trailing newline, got %q", line)
		// Verify it's valid JSON that decodes to the same payload.
		var got wal.Data
		require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(line)), &got))
		require.Equal(t, "I", got.Action)
		require.Equal(t, "public", got.Schema)
		require.Equal(t, "users", got.Table)
	})

	t.Run("invokes checkpoint after data event", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		var gotPositions []wal.CommitPosition
		ckpt := func(_ context.Context, p []wal.CommitPosition) error {
			gotPositions = append(gotPositions, p...)
			return nil
		}
		w := NewWriter(WithWriter(&buf), WithCheckpoint(ckpt))

		require.NoError(t, w.ProcessWALEvent(context.Background(), dataEvent))
		require.Equal(t, []wal.CommitPosition{dataEvent.CommitPosition}, gotPositions)
	})

	t.Run("checkpoints keep-alive event without writing", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		var called bool
		ckpt := func(_ context.Context, _ []wal.CommitPosition) error {
			called = true
			return nil
		}
		w := NewWriter(WithWriter(&buf), WithCheckpoint(ckpt))

		require.NoError(t, w.ProcessWALEvent(context.Background(), keepAliveEvent))
		require.Empty(t, buf.String())
		require.True(t, called, "expected checkpoint to be called for keep-alive")
	})

	t.Run("checkpoint error does not fail the event", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		ckpt := func(_ context.Context, _ []wal.CommitPosition) error {
			return errors.New("boom")
		}
		w := NewWriter(WithWriter(&buf), WithCheckpoint(ckpt))

		require.NoError(t, w.ProcessWALEvent(context.Background(), dataEvent))
		require.NotEmpty(t, buf.String())
	})
}

func TestWriter_NameAndClose(t *testing.T) {
	t.Parallel()
	w := NewWriter()
	require.Equal(t, "stdout-writer", w.Name())
	require.NoError(t, w.Close())
}
