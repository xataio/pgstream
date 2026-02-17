// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstoremocks "github.com/xataio/pgstream/pkg/snapshot/store/mocks"
)

// TestSnapshotRecorder_MarkInProgress_BoundedConcurrency verifies that
// markSnapshotInProgress limits concurrent store operations to
// maxStoreConnections. Without this limit, N schemas would open N simultaneous
// connections from the snapshot-store pool — e.g., 41 schemas would hold 41
// connections just for bookkeeping INSERT/UPDATEs, exhausting the source
// database's connection slots before the data snapshot even starts.
func TestSnapshotRecorder_MarkInProgress_BoundedConcurrency(t *testing.T) {
	t.Parallel()

	numSchemas := 41 // mirrors Zennagents (41 schemas, hundreds of tables)
	schemaTables := make(map[string][]string, numSchemas)
	for i := 0; i < numSchemas; i++ {
		schemaTables[fmt.Sprintf("schema_%d", i)] = []string{"table_a", "table_b"}
	}

	ss := &snapshot.Snapshot{
		SchemaTables: schemaTables,
	}

	// Track peak concurrent store operations — each represents a connection
	// held from the snapshot-store pool.
	var concurrentOps atomic.Int64
	var peakConcurrentOps atomic.Int64

	trackConcurrency := func() func() {
		current := concurrentOps.Add(1)
		for {
			peak := peakConcurrentOps.Load()
			if current <= peak || peakConcurrentOps.CompareAndSwap(peak, current) {
				break
			}
		}
		return func() {
			concurrentOps.Add(-1)
		}
	}

	store := &snapshotstoremocks.Store{
		GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
			return []*snapshot.Request{}, nil
		},
		CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
			release := trackConcurrency()
			defer release()
			// Simulate a real store operation hitting the database.
			// Without this delay, goroutines may complete before others start,
			// masking the concurrency.
			time.Sleep(50 * time.Millisecond)
			return nil
		},
		UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
			return nil
		},
	}

	generator := &mockGenerator{
		createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
			return nil
		},
	}

	recorder := NewSnapshotRecorder(store, generator, true)
	defer recorder.Close()

	err := recorder.CreateSnapshot(context.Background(), ss)
	require.NoError(t, err)

	peak := peakConcurrentOps.Load()
	t.Logf("Schemas: %d, Peak concurrent store operations: %d, Limit: %d", numSchemas, peak, maxStoreConnections)

	require.LessOrEqual(t, peak, int64(maxStoreConnections),
		"Store concurrency must not exceed maxStoreConnections=%d, but peaked at %d. "+
			"With %d schemas, unbounded concurrency would open %d connections simultaneously.",
		maxStoreConnections, peak, numSchemas, numSchemas)

	// Sanity: concurrency should actually be used (not accidentally serialized)
	require.GreaterOrEqual(t, peak, int64(2),
		"Expected at least 2 concurrent operations to confirm parallelism is working")
}
