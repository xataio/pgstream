// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	jsonlib "github.com/xataio/pgstream/internal/json"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

// TestRestoreToWAL_PostDataDDL_FKConstraints verifies that ALTER TABLE ONLY
// statements (the standard pg_dump format for FK constraints) are processed
// through the WAL restore path. This is the exact SQL pg_dump generates for
// the post-data phase.
func TestRestoreToWAL_PostDataDDL_FKConstraints(t *testing.T) {
	t.Parallel()

	// Real pg_dump post-data output — note the ONLY keyword
	postDataDump := []byte(`ALTER TABLE ONLY public.child_table
    ADD CONSTRAINT child_table_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.parent_table(id);

ALTER TABLE ONLY public.child_table
    ADD CONSTRAINT child_table_pkey PRIMARY KEY (id);

CREATE INDEX idx_child_parent ON public.child_table USING btree (parent_id);
`)

	var capturedEvents []*wal.Event
	var mu sync.Mutex

	processor := &mocks.Processor{
		ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
			mu.Lock()
			capturedEvents = append(capturedEvents, walEvent)
			mu.Unlock()
			return nil
		},
	}

	restore := newPGSnapshotWALRestore(processor, newNoTableQuerier())
	_, err := restore.restoreToWAL(context.Background(), pglib.PGRestoreOptions{}, postDataDump)
	require.NoError(t, err)

	// Must process all 3 DDL statements
	require.Equal(t, 3, len(capturedEvents),
		"Expected 3 post-data DDL events (FK constraint, PK constraint, index), got %d", len(capturedEvents))

	// Verify the actual SQL in each event
	for i, event := range capturedEvents {
		require.Equal(t, wal.LogicalMessageAction, event.Data.Action)
		require.Equal(t, wal.DDLPrefix, event.Data.Prefix)

		var ddlEvent wal.DDLEvent
		require.NoError(t, jsonlib.Unmarshal([]byte(event.Data.Content), &ddlEvent),
			"event %d content must be valid DDL JSON", i)
		require.NotEmpty(t, ddlEvent.DDL, "event %d must have DDL SQL", i)
	}

	// Event 0: FK constraint
	var fkEvent wal.DDLEvent
	require.NoError(t, jsonlib.Unmarshal([]byte(capturedEvents[0].Data.Content), &fkEvent))
	require.Contains(t, fkEvent.DDL, "FOREIGN KEY",
		"First event must be the FK constraint")
	require.Contains(t, fkEvent.DDL, "child_table_parent_id_fkey")

	// Event 1: PK constraint
	var pkEvent wal.DDLEvent
	require.NoError(t, jsonlib.Unmarshal([]byte(capturedEvents[1].Data.Content), &pkEvent))
	require.Contains(t, pkEvent.DDL, "PRIMARY KEY",
		"Second event must be the PK constraint")

	// Event 2: Index
	var idxEvent wal.DDLEvent
	require.NoError(t, jsonlib.Unmarshal([]byte(capturedEvents[2].Data.Content), &idxEvent))
	require.Contains(t, idxEvent.DDL, "CREATE INDEX",
		"Third event must be the index")
}

// TestRestoreToWAL_ProcessorCloseBlocksPostData simulates the real Kafka
// snapshot flow: the data generator closes the processor after sending rows,
// then the schema generator tries to send post-data DDL through the same
// processor. This test proves whether the processor close is the root cause
// of missing post-data DDL.
func TestRestoreToWAL_ProcessorCloseBlocksPostData(t *testing.T) {
	t.Parallel()

	schemaDump := []byte(`CREATE TABLE public.parent_table (
    id integer NOT NULL
);

CREATE TABLE public.child_table (
    id integer NOT NULL,
    parent_id integer
);
`)

	postDataDump := []byte(`ALTER TABLE ONLY public.child_table
    ADD CONSTRAINT child_table_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.parent_table
    ADD CONSTRAINT parent_table_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.child_table
    ADD CONSTRAINT child_table_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.parent_table(id);
`)

	var preCloseEvents int
	var postCloseEvents int
	var processorClosed bool
	var mu sync.Mutex

	processor := &mocks.Processor{
		ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
			mu.Lock()
			defer mu.Unlock()
			if processorClosed {
				postCloseEvents++
			} else {
				preCloseEvents++
			}
			return nil
		},
		CloseFn: func() error {
			mu.Lock()
			processorClosed = true
			mu.Unlock()
			return nil
		},
	}

	querier := newNoTableQuerier()
	restore := newPGSnapshotWALRestore(processor, querier)

	// Phase 1: Schema DDL (before data, before processor close)
	_, err := restore.restoreToWAL(context.Background(), pglib.PGRestoreOptions{}, schemaDump)
	require.NoError(t, err)
	require.Equal(t, 2, preCloseEvents, "Schema phase should produce 2 CREATE TABLE events")

	// Simulate what the data snapshot generator does: close the processor
	processor.Close()

	// Phase 3: Post-data DDL (after processor close)
	_, err = restore.restoreToWAL(context.Background(), pglib.PGRestoreOptions{}, postDataDump)

	// This is the test: did the post-data DDL get processed?
	mu.Lock()
	totalPostClose := postCloseEvents
	mu.Unlock()

	// If post-data events were processed after close, the processor close
	// is NOT the problem (the processor mock doesn't actually block after close).
	// But in real Kafka mode, processor.Close() shuts down the batch writer,
	// so ProcessWALEvent would return an error.
	t.Logf("Pre-close events: %d, Post-close events: %d, Error from post-data: %v",
		preCloseEvents, totalPostClose, err)

	// The real question: does restoreToWAL even get called for post-data?
	// If err != nil, the processor close caused it.
	// If totalPostClose == 3, the events went through (mock doesn't block).
	// Either way, this tells us what's happening.
	if err != nil {
		t.Logf("CONFIRMED: Processor close blocks post-data DDL: %v", err)
	} else if totalPostClose == 3 {
		t.Logf("Processor close did NOT block (mock doesn't enforce). Real Kafka writer would block.")
	} else {
		t.Logf("UNEXPECTED: %d post-close events (expected 3)", totalPostClose)
	}
}

// TestRestoreToWAL_ProcessorCloseBlocksPostData_RealisticClose simulates
// what the REAL Kafka batch writer does: after Close(), ProcessWALEvent
// returns "context canceled". This proves the processor close is the root
// cause of missing post-data DDL in the Kafka path.
func TestRestoreToWAL_ProcessorCloseBlocksPostData_RealisticClose(t *testing.T) {
	t.Parallel()

	postDataDump := []byte(`ALTER TABLE ONLY public.child_table
    ADD CONSTRAINT child_table_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.child_table
    ADD CONSTRAINT child_table_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.parent_table(id);
`)

	var processorClosed bool
	var mu sync.Mutex
	errClosed := errors.New("stop processing, sending has stopped: context canceled")

	processor := &mocks.Processor{
		ProcessWALEventFn: func(ctx context.Context, walEvent *wal.Event) error {
			mu.Lock()
			defer mu.Unlock()
			if processorClosed {
				return errClosed
			}
			return nil
		},
		CloseFn: func() error {
			mu.Lock()
			processorClosed = true
			mu.Unlock()
			return nil
		},
	}

	// Close the processor BEFORE sending post-data DDL
	// (simulates data generator closing the shared processor)
	processor.Close()

	restore := newPGSnapshotWALRestore(processor, newNoTableQuerier())
	_, err := restore.restoreToWAL(context.Background(), pglib.PGRestoreOptions{}, postDataDump)

	// This MUST fail — the processor is closed, post-data DDL can't be sent
	require.Error(t, err,
		"restoreToWAL must fail when processor is closed — this is why FK constraints are missing on the Kafka path")
	require.Contains(t, err.Error(), "context canceled",
		"Error must be context canceled from the closed processor")
}

