// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"fmt"
	"time"

	"github.com/jonboulle/clockwork"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/progress"
	synclib "github.com/xataio/pgstream/internal/sync"
)

// snapshotTracker tracks the progress of long-running operations during a
// pg restore, such as index creation, and displays progress bars.
type snapshotTracker struct {
	conn         pglib.Querier
	progressBars *synclib.Map[int, progress.Bar]
	barBuilder   func(total int64, description, unit string) progress.Bar
	clock        clockwork.Clock
}

// indexCreationRow representation of a row from pg_stat_progress_create_index
type indexCreationRow struct {
	// PID of the backend building the index.
	PID int
	// Table on which the index is being created.
	Table string
	// OID of the index being created or reindexed. During a non-concurrent CREATE INDEX, this is 0.
	Index string
	// Current processing phase of index creation.
	Phase string
	// Number of tuples already processed in the current phase.
	TuplesDone int64
	// Total number of tuples to be processed in the current phase.
	TuplesTotal int64
	// Specific command type: CREATE INDEX, CREATE INDEX CONCURRENTLY, REINDEX, or REINDEX CONCURRENTLY.
	Command string
}

// indexProgressCheckInterval defines how often to check for index creation progress.
const indexProgressCheckInterval = time.Millisecond * 500

func newSnapshotTracker(ctx context.Context, pgurl string) (*snapshotTracker, error) {
	connPool, err := pglib.NewConnPool(ctx, pgurl)
	if err != nil {
		return nil, err
	}
	return &snapshotTracker{
		conn:         connPool,
		progressBars: synclib.NewMap[int, progress.Bar](),
		clock:        clockwork.NewRealClock(),
		barBuilder:   progress.NewBar,
	}, nil
}

func (st *snapshotTracker) trackIndexesCreation(ctx context.Context) {
	ticker := st.clock.NewTicker(indexProgressCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			for pid := range st.progressBars.GetMap() {
				st.markProgressBarCompleted(pid)
			}
			return
		case <-ticker.Chan():
			rowMap, err := st.getCreateIndexProgressRows(ctx)
			if err != nil || len(rowMap) == 0 {
				continue
			}

			for pid, row := range rowMap {
				// skip initialization phase where total is 0
				if row.TuplesTotal == 0 {
					continue
				}

				// We can't use the index oid in the row to uniquely identify
				// the index being tracked since it is not set for CREATE INDEX
				// which is the command the restore produces. Instead we use the
				// pid of the backend building it: a backend runs one statement
				// at a time, so a single bar tracks it. When the number of
				// tuples done drops below the previous value, the backend has
				// moved on to the next index and the previous one is complete.
				// Keying by table instead would collapse the bars of indices
				// that concurrent restores build on the same table.
				existingBar, found := st.progressBars.Get(pid)
				switch {
				case found && row.TuplesDone >= existingBar.Current():
					existingBar.SetCurrent(row.TuplesDone)
					continue
				case found && row.TuplesDone < existingBar.Current():
					// if we're setting a lower current value, it's likely that
					// a new index creation has started on the same backend. So
					// complete the old bar and create a new one.
					st.markProgressBarCompleted(pid)
					fallthrough
				default:
					// Create new progress bar for the index being created if not
					// found in the bar map
					bar := st.barBuilder(row.TuplesTotal, st.barDescription(row.Table), "tuples")
					st.progressBars.Set(pid, bar)
					bar.SetCurrent(row.TuplesDone)
				}
			}

			// when the rows no longer return an index being tracked, it means
			// the index creation is done and we can mark it as complete.
			for pid := range st.progressBars.GetMap() {
				if _, found := rowMap[pid]; !found {
					st.markProgressBarCompleted(pid)
				}
			}
		}
	}
}

// https://www.postgresql.org/docs/current/progress-reporting.html#CREATE-INDEX-PROGRESS-REPORTING
const createIndexProgressQuery = `SELECT pid, relid::regclass AS table,index_relid::regclass AS index, phase, tuples_done, tuples_total, command FROM pg_stat_progress_create_index;`

func (st *snapshotTracker) getCreateIndexProgressRows(ctx context.Context) (map[int]indexCreationRow, error) {
	rows, err := st.conn.Query(ctx, createIndexProgressQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[int]indexCreationRow{}
	for rows.Next() {
		var row indexCreationRow
		if err := rows.Scan(&row.PID, &row.Table, &row.Index, &row.Phase, &row.TuplesDone, &row.TuplesTotal, &row.Command); err != nil {
			return nil, err
		}
		result[row.PID] = row
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (st *snapshotTracker) markProgressBarCompleted(pid int) {
	bar, found := st.progressBars.Get(pid)
	if found {
		bar.Close()
	}
	st.progressBars.Delete(pid)
}

func (st *snapshotTracker) barDescription(table string) string {
	return fmt.Sprintf("[cyan][%s][reset] Restoring index...", table)
}

func (st *snapshotTracker) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return st.conn.Close(ctx)
}
