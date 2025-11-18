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
	progressBars *synclib.Map[string, progress.Bar]
	barBuilder   func(total int64, description, unit string) progress.Bar
	clock        clockwork.Clock
}

// indexCreationRow representation of a row from pg_stat_progress_create_index
type indexCreationRow struct {
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
		progressBars: synclib.NewMap[string, progress.Bar](),
		clock:        clockwork.NewRealClock(),
		barBuilder: func(total int64, description, unit string) progress.Bar {
			return progress.NewBar(total, description, unit)
		},
	}, nil
}

func (st *snapshotTracker) trackIndexesCreation(ctx context.Context) {
	ticker := st.clock.NewTicker(indexProgressCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			for table := range st.progressBars.GetMap() {
				st.markProgressBarCompleted(table)
			}
			return
		case <-ticker.Chan():
			rowMap, err := st.getCreateIndexProgressRows(ctx)
			if err != nil || len(rowMap) == 0 {
				continue
			}

			for table, row := range rowMap {
				// skip initialization phase where total is 0
				if row.TuplesTotal == 0 {
					continue
				}

				// We can't use the index oid in the row to uniquely identify
				// the index being tracked since it is not set for CREATE INDEX
				// which is the command the restore produces. Instead we use the
				// table name.
				//
				// There can only be one index being created per table at a
				// time, so we can track progress bars by table name. When the
				// number of tuples done is lower than the previous recorded
				// value, it means a new index is being created for the same
				// table and the previous one can be marked as completed.
				existingBar, found := st.progressBars.Get(table)
				switch {
				case found && row.TuplesDone >= existingBar.Current():
					existingBar.SetCurrent(row.TuplesDone)
					continue
				case found && row.TuplesDone < existingBar.Current():
					// if we're setting a lower current value, it's likely that
					// a new index creation has started on the same table. So
					// complete the old bar and create a new one.
					st.markProgressBarCompleted(table)
					fallthrough
				default:
					// Create new progress bar for the index being created if not
					// found in the bar map
					bar := st.barBuilder(row.TuplesTotal, st.barDescription(row.Table), "tuples")
					st.progressBars.Set(row.Table, bar)
					bar.SetCurrent(row.TuplesDone)
				}
			}

			// when the rows no longer return an existing table index being tracked,
			// it means the index creation is done and we can mark it as
			// complete.
			for table := range st.progressBars.GetMap() {
				if _, found := rowMap[table]; !found {
					st.markProgressBarCompleted(table)
				}
			}
		}
	}
}

// https://www.postgresql.org/docs/current/progress-reporting.html#CREATE-INDEX-PROGRESS-REPORTING
const createIndexProgressQuery = `SELECT relid::regclass AS table,index_relid::regclass AS index, phase, tuples_done, tuples_total, command FROM pg_stat_progress_create_index;`

func (st *snapshotTracker) getCreateIndexProgressRows(ctx context.Context) (map[string]indexCreationRow, error) {
	rows, err := st.conn.Query(ctx, createIndexProgressQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[string]indexCreationRow{}
	for rows.Next() {
		var row indexCreationRow
		if err := rows.Scan(&row.Table, &row.Index, &row.Phase, &row.TuplesDone, &row.TuplesTotal, &row.Command); err != nil {
			return nil, err
		}
		result[row.Table] = row
	}
	return result, nil
}

func (st *snapshotTracker) markProgressBarCompleted(name string) {
	bar, found := st.progressBars.Get(name)
	if found {
		bar.Close()
	}
	st.progressBars.Delete(name)
}

func (st *snapshotTracker) barDescription(table string) string {
	return fmt.Sprintf("[cyan][%s][reset] Restoring index...", table)
}

func (st *snapshotTracker) close() error {
	return st.conn.Close(context.Background())
}
