// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
)

// SnapshotRecorder is a decorator around a snapshot generator that will record
// the snapshot request status.
type SnapshotRecorder struct {
	wrapped SnapshotGenerator
	store   snapshotstore.Store
}

const updateTimeout = time.Minute

// NewSnapshotRecorder will return the generator on input wrapped with an
// activity recorder that will keep track of the status of the snapshot
// requests.
func NewSnapshotRecorder(store snapshotstore.Store, generator SnapshotGenerator) *SnapshotRecorder {
	return &SnapshotRecorder{
		wrapped: generator,
		store:   store,
	}
}

func (s *SnapshotRecorder) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	filteredTables, err := s.filterOutExistingSnapshots(ctx, ss.SchemaName, ss.TableNames)
	if err != nil {
		return snapshot.NewErrors(err)
	}
	// no tables to snapshot
	if len(filteredTables) == 0 {
		return nil
	}

	ss.TableNames = filteredTables
	req := &snapshot.Request{
		Snapshot: *ss,
	}

	if err := s.markSnapshotInProgress(ctx, req); err != nil {
		return snapshot.NewErrors(err)
	}

	err = s.wrapped.CreateSnapshot(ctx, ss)

	return s.markSnapshotCompleted(req, err)
}

func (s *SnapshotRecorder) Close() error {
	s.store.Close()
	return s.wrapped.Close()
}

func (s *SnapshotRecorder) markSnapshotInProgress(ctx context.Context, req *snapshot.Request) error {
	if err := s.store.CreateSnapshotRequest(ctx, req); err != nil {
		return err
	}
	// the snapshot will start immediately
	req.MarkInProgress()
	return s.store.UpdateSnapshotRequest(ctx, req)
}

func (s *SnapshotRecorder) markSnapshotCompleted(req *snapshot.Request, err error) error {
	// make sure we can update the request status in the store regardless of
	// context cancelations
	ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)
	defer cancel()

	req.MarkCompleted(err)
	if updateErr := s.store.UpdateSnapshotRequest(ctx, req); updateErr != nil {
		if err == nil {
			return snapshot.NewErrors(updateErr)
		}
		snapshotErr := snapshot.NewErrors(err)
		snapshotErr.AddSnapshotError(updateErr)
		return snapshotErr
	}
	return err
}

func (s *SnapshotRecorder) filterOutExistingSnapshots(ctx context.Context, schema string, tables []string) ([]string, error) {
	snapshotRequests, err := s.store.GetSnapshotRequestsBySchema(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("retrieving existing snapshots for schema: %w", err)
	}

	if len(snapshotRequests) == 0 {
		return tables, nil
	}

	existingRequests := map[string]*snapshot.Request{}
	for _, req := range snapshotRequests {
		for _, table := range req.Snapshot.TableNames {
			existingRequests[table] = req
		}
	}

	const wildcard = "*"
	filteredTables := make([]string, 0, len(tables))
	for _, table := range tables {
		wildCardReq, wildcardFound := existingRequests[wildcard]

		switch table {
		case wildcard:
			if wildcardFound {
				filteredTables = append(filteredTables, wildCardReq.Errors.GetFailedTables()...)
				break
			}
			filteredTables = append(filteredTables, table)
		default:
			tableReq, tableFound := existingRequests[table]

			if (tableFound && tableReq.HasFailedForTable(table)) ||
				(wildcardFound && wildCardReq.HasFailedForTable(table)) ||
				(!tableFound && !wildcardFound) {
				filteredTables = append(filteredTables, table)
			}
		}
	}
	return filteredTables, nil
}
