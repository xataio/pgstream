// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	"golang.org/x/sync/errgroup"
)

// SnapshotRecorder is a decorator around a snapshot generator that will record
// the snapshot request status.
type SnapshotRecorder struct {
	wrapped             SnapshotGenerator
	store               snapshotstore.Store
	repeatableSnapshots bool
	schemaWorkers       uint
}

type Config struct {
	RepeatableSnapshots bool
	SnapshotWorkers     uint
}

const (
	defaultSnapshotWorkers = 1
	updateTimeout          = time.Minute
)

// NewSnapshotRecorder will return the generator on input wrapped with an
// activity recorder that will keep track of the status of the snapshot
// requests.
func NewSnapshotRecorder(cfg *Config, store snapshotstore.Store, generator SnapshotGenerator) *SnapshotRecorder {
	return &SnapshotRecorder{
		wrapped:             generator,
		store:               store,
		repeatableSnapshots: cfg.RepeatableSnapshots,
		schemaWorkers:       cfg.snapshotWorkers(),
	}
}

func (s *SnapshotRecorder) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	var err error
	ss.SchemaTables, err = s.filterOutExistingSnapshots(ctx, ss.SchemaTables)
	if err != nil {
		return err
	}

	// no tables to snapshot
	if !ss.HasTables() {
		return nil
	}

	requests := s.createRequests(ss)

	if err := s.markSnapshotInProgress(ctx, requests); err != nil {
		return err
	}

	err = s.wrapped.CreateSnapshot(ctx, ss)

	return s.markSnapshotCompleted(ctx, requests, err)
}

func (s *SnapshotRecorder) Close() error {
	s.store.Close()
	return s.wrapped.Close()
}

func (s *SnapshotRecorder) createRequests(ss *snapshot.Snapshot) []*snapshot.Request {
	requests := make([]*snapshot.Request, 0, len(ss.SchemaTables))
	for schema, tables := range ss.SchemaTables {
		req := &snapshot.Request{
			Schema: schema,
			Tables: tables,
		}
		requests = append(requests, req)
	}
	return requests
}

func (s *SnapshotRecorder) markSnapshotInProgress(ctx context.Context, requests []*snapshot.Request) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(int(s.schemaWorkers))

	// create one request per schema
	for _, req := range requests {
		eg.Go(func() error {
			if err := s.store.CreateSnapshotRequest(ctx, req); err != nil {
				return err
			}
			// the snapshot will start immediately
			req.MarkInProgress()
			return s.store.UpdateSnapshotRequest(ctx, req)
		})
	}

	return eg.Wait()
}

func (s *SnapshotRecorder) markSnapshotCompleted(ctx context.Context, requests []*snapshot.Request, err error) error {
	// make sure we can update the request status in the store regardless of
	// context cancelations
	if ctx.Err() != nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, updateTimeout)
	defer cancel()

	getSchemaErrors := func(schema string, err error) *snapshot.SchemaErrors {
		if err == nil {
			return nil
		}
		snapshotErr := &snapshot.Errors{}
		if errors.As(err, &snapshotErr) {
			return snapshotErr.GetSchemaErrors(schema)
		}
		return snapshot.NewSchemaErrors(schema, err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(int(s.schemaWorkers))

	for _, req := range requests {
		eg.Go(func() error {
			schemaErrs := getSchemaErrors(req.Schema, err)
			req.MarkCompleted(req.Schema, schemaErrs)
			if updateErr := s.store.UpdateSnapshotRequest(ctx, req); updateErr != nil {
				if err == nil {
					return updateErr
				}
				schemaErrs.AddGlobalError(updateErr)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return err
}

func (s *SnapshotRecorder) filterOutExistingSnapshots(ctx context.Context, schemaTables map[string][]string) (map[string][]string, error) {
	// if we want to be able to repeat snapshots, we don't filter out existing ones
	if s.repeatableSnapshots {
		return schemaTables, nil
	}

	filteredSchemaTables := make(map[string][]string, len(schemaTables))
	for schema, tables := range schemaTables {
		filteredTables, err := s.filterOutExistingSchemaTables(ctx, schema, tables)
		if err != nil {
			return nil, fmt.Errorf("filtering existing snapshots for schema %s: %w", schema, err)
		}
		if len(filteredTables) > 0 {
			filteredSchemaTables[schema] = filteredTables
		}
	}
	return filteredSchemaTables, nil
}

func (s *SnapshotRecorder) filterOutExistingSchemaTables(ctx context.Context, schema string, tables []string) ([]string, error) {
	snapshotRequests, err := s.store.GetSnapshotRequestsBySchema(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("retrieving existing snapshots for schema: %w", err)
	}

	if len(snapshotRequests) == 0 {
		return tables, nil
	}

	existingRequests := map[string]*snapshot.Request{}
	for _, req := range snapshotRequests {
		for _, table := range req.Tables {
			existingRequests[table] = req
		}
	}

	const wildcard = "*"
	filteredTables := make([]string, 0, len(tables))
	for _, table := range tables {
		wildCardReq, wildcardFound := existingRequests[wildcard]
		switch table {
		case wildcard:
			if wildcardFound && !wildCardReq.Errors.IsGlobalError() {
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

func (c *Config) snapshotWorkers() uint {
	if c.SnapshotWorkers <= 0 {
		return defaultSnapshotWorkers
	}
	return c.SnapshotWorkers
}
