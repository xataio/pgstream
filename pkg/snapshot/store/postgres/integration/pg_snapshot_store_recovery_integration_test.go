// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	"github.com/xataio/pgstream/pkg/snapshot/store"
	pgstore "github.com/xataio/pgstream/pkg/snapshot/store/postgres"
)

// Test_SnapshotStore_LockSnapshotRequests checks that only one holder can lock
// a schema and table list at a time, whatever the request mode, and that the
// lock is free again once released.
func Test_SnapshotStore_LockSnapshotRequests(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()
	s, err := pgstore.New(ctx, sharedPGURL)
	require.NoError(t, err)
	defer s.Close()

	dataReq := &snapshot.Request{Schema: "lock_schema", Tables: []string{"t1"}, Mode: snapshot.RequestModeData}
	schemaOnlyReq := &snapshot.Request{Schema: "lock_schema", Tables: []string{"t1"}, Mode: snapshot.RequestModeSchemaOnly}
	otherReq := &snapshot.Request{Schema: "lock_schema", Tables: []string{"t2"}}

	release, err := s.LockSnapshotRequests(ctx, []*snapshot.Request{dataReq})
	require.NoError(t, err)

	// the unique index ignores mode, so the lock does too
	_, err = s.LockSnapshotRequests(ctx, []*snapshot.Request{schemaOnlyReq})
	require.ErrorIs(t, err, store.ErrSnapshotRequestLocked)

	// a different table list is a different scope
	releaseOther, err := s.LockSnapshotRequests(ctx, []*snapshot.Request{otherReq})
	require.NoError(t, err)
	releaseOther()

	release()
	release, err = s.LockSnapshotRequests(ctx, []*snapshot.Request{schemaOnlyReq})
	require.NoError(t, err)
	release()
}

// Test_SnapshotStore_LockReleasedWhenHolderDies checks that the server frees
// the lock of a run whose connection is gone, as after a kill -9, so a later
// run can recover its request.
func Test_SnapshotStore_LockReleasedWhenHolderDies(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()
	s, err := pgstore.New(ctx, sharedPGURL)
	require.NoError(t, err)
	defer s.Close()

	req := &snapshot.Request{Schema: "lock_dies_schema", Tables: []string{"t1"}}
	release, err := s.LockSnapshotRequests(ctx, []*snapshot.Request{req})
	require.NoError(t, err)
	defer release()

	_, err = s.LockSnapshotRequests(ctx, []*snapshot.Request{req})
	require.ErrorIs(t, err, store.ErrSnapshotRequestLocked)

	// end the holder's session from outside, like a dead process would
	conn, err := pglib.NewConn(ctx, sharedPGURL)
	require.NoError(t, err)
	defer conn.Close(ctx)
	var terminated int
	err = conn.QueryRow(ctx, []any{&terminated}, fmt.Sprintf(`SELECT count(pg_terminate_backend(l.pid))::int FROM pg_locks l
	WHERE l.locktype = 'advisory' AND l.granted
	AND ((l.classid::bigint << 32) | l.objid::bigint) = hashtextextended($1::text || '/' || %s.snapshot_table_names_hash($2::text[]), 0)`, store.SchemaName),
		req.Schema, req.Tables)
	require.NoError(t, err)
	require.Equal(t, 1, terminated)

	require.Eventually(t, func() bool {
		release, err := s.LockSnapshotRequests(ctx, []*snapshot.Request{req})
		if err != nil {
			return false
		}
		release()
		return true
	}, 10*time.Second, 100*time.Millisecond)
}

// Test_SnapshotStore_ResumeSnapshotRequest checks the take over of a leftover
// request: it matches the unique index key whatever the mode, records the new
// mode so the completion update finds the row, and reports when there is
// nothing left to take over.
func Test_SnapshotStore_ResumeSnapshotRequest(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()
	clearRequests(t, "resume_schema")

	s, err := pgstore.New(ctx, sharedPGURL)
	require.NoError(t, err)
	defer s.Close()

	// a schema-only run left its request behind
	leftover := &snapshot.Request{Schema: "resume_schema", Tables: []string{"t1"}, Mode: snapshot.RequestModeSchemaOnly}
	require.NoError(t, s.CreateSnapshotRequest(ctx, leftover))

	// a data run for the same tables conflicts on the index, which ignores mode
	req := &snapshot.Request{Schema: "resume_schema", Tables: []string{"t1"}, Mode: snapshot.RequestModeData}
	require.ErrorIs(t, s.CreateSnapshotRequest(ctx, req), store.ErrNonCompletedRequestExists)

	resumed, err := s.ResumeSnapshotRequest(ctx, req)
	require.NoError(t, err)
	require.True(t, resumed)
	requireRequests(t, s, "resume_schema", []snapshot.Status{snapshot.StatusInProgress}, snapshot.RequestModeData)

	// the completion update is keyed on mode and must find the row
	req.MarkCompleted(req.Schema, nil)
	require.NoError(t, s.UpdateSnapshotRequest(ctx, req))
	requireRequests(t, s, "resume_schema", []snapshot.Status{snapshot.StatusCompleted}, snapshot.RequestModeData)

	// nothing non-completed is left, so there is nothing to take over
	resumed, err = s.ResumeSnapshotRequest(ctx, req)
	require.NoError(t, err)
	require.False(t, resumed)
}

// Test_SnapshotRecorder_ConcurrentRuns runs the recorder against real postgres
// with repeatable snapshots. While a slow run is copying, two retries started
// together are refused and leave its request alone, so neither can complete
// it early and the slow run's own completion updates its own row. A run that
// died (its request is left in progress and its lock is gone) is recovered.
func Test_SnapshotRecorder_ConcurrentRuns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()
	const schema = "concurrent_schema"
	clearRequests(t, schema)

	newRecorder := func(gen generator.SnapshotGenerator) *generator.SnapshotRecorder {
		s, err := pgstore.New(ctx, sharedPGURL)
		require.NoError(t, err)
		return generator.NewSnapshotRecorder(&generator.Config{RepeatableSnapshots: true}, s, gen)
	}
	newSnapshot := func() *snapshot.Snapshot {
		return &snapshot.Snapshot{SchemaTables: map[string][]string{schema: {"t1"}}}
	}

	s, err := pgstore.New(ctx, sharedPGURL)
	require.NoError(t, err)
	defer s.Close()

	// slow writer: blocks until told to finish
	started := make(chan struct{})
	finish := make(chan struct{})
	slow := newRecorder(&blockingGenerator{started: started, finish: finish})
	defer slow.Close()
	slowErr := make(chan error, 1)
	go func() { slowErr <- slow.CreateSnapshot(ctx, newSnapshot()) }()
	<-started
	requireRequests(t, s, schema, []snapshot.Status{snapshot.StatusInProgress}, snapshot.RequestModeData)

	// two retries at the same time while the slow run is still copying. The
	// recorders are built first, since building a store runs its table setup.
	retryGens := []*recordingGenerator{{}, {}}
	retries := make([]*generator.SnapshotRecorder, 0, len(retryGens))
	for _, gen := range retryGens {
		r := newRecorder(gen)
		defer r.Close()
		retries = append(retries, r)
	}
	var wg sync.WaitGroup
	retryErrs := make([]error, len(retries))
	for i, r := range retries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			retryErrs[i] = r.CreateSnapshot(ctx, newSnapshot())
		}()
	}
	wg.Wait()
	for i := range retries {
		require.ErrorIs(t, retryErrs[i], store.ErrSnapshotRequestLocked)
		require.False(t, retryGens[i].called, "a refused retry must not copy")
	}
	// the slow run's request is untouched by the refused retries
	requireRequests(t, s, schema, []snapshot.Status{snapshot.StatusInProgress}, snapshot.RequestModeData)

	close(finish)
	require.NoError(t, <-slowErr)
	requireRequests(t, s, schema, []snapshot.Status{snapshot.StatusCompleted}, snapshot.RequestModeData)

	// a run that died: its request stays in progress and nobody holds its lock
	dead := &snapshot.Request{Schema: schema, Tables: []string{"t1"}, Mode: snapshot.RequestModeData}
	require.NoError(t, s.CreateSnapshotRequest(ctx, dead))
	dead.MarkInProgress()
	require.NoError(t, s.UpdateSnapshotRequest(ctx, dead))

	gen := &recordingGenerator{}
	recovery := newRecorder(gen)
	defer recovery.Close()
	require.NoError(t, recovery.CreateSnapshot(ctx, newSnapshot()))
	require.True(t, gen.called)
	requireRequests(t, s, schema, []snapshot.Status{snapshot.StatusCompleted, snapshot.StatusCompleted}, snapshot.RequestModeData)
}

type blockingGenerator struct {
	started chan struct{}
	finish  chan struct{}
}

func (g *blockingGenerator) CreateSnapshot(ctx context.Context, _ *snapshot.Snapshot) error {
	close(g.started)
	<-g.finish
	return nil
}

func (g *blockingGenerator) Close() error { return nil }

type recordingGenerator struct {
	called bool
}

func (g *recordingGenerator) CreateSnapshot(context.Context, *snapshot.Snapshot) error {
	g.called = true
	return nil
}

func (g *recordingGenerator) Close() error { return nil }

func requireRequests(t *testing.T, s *pgstore.Store, schema string, wantStatuses []snapshot.Status, wantMode snapshot.RequestMode) {
	t.Helper()

	reqs, err := s.GetSnapshotRequestsBySchema(context.Background(), schema)
	require.NoError(t, err)
	statuses := make([]snapshot.Status, 0, len(reqs))
	for _, req := range reqs {
		statuses = append(statuses, req.Status)
		require.Equal(t, wantMode, req.GetMode())
	}
	require.Equal(t, wantStatuses, statuses)
}
