// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	"golang.org/x/sync/errgroup"
)

// SnapshotRecorder is a decorator around a snapshot generator that will record
// the snapshot request status.
type SnapshotRecorder struct {
	logger              loglib.Logger
	wrapped             SnapshotGenerator
	store               snapshotstore.Store
	repeatableSnapshots bool
	schemaWorkers       uint
}

type Option func(*SnapshotRecorder)

func WithLogger(logger loglib.Logger) Option {
	return func(s *SnapshotRecorder) {
		s.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "snapshot_generator_recorder",
		})
	}
}

type Config struct {
	RepeatableSnapshots bool
	SnapshotWorkers     uint
}

const (
	defaultSnapshotWorkers = 1
	updateTimeout          = time.Minute

	wildcard = "*"
)

// NewSnapshotRecorder will return the generator on input wrapped with an
// activity recorder that will keep track of the status of the snapshot
// requests.
func NewSnapshotRecorder(cfg *Config, store snapshotstore.Store, generator SnapshotGenerator, opts ...Option) *SnapshotRecorder {
	sr := &SnapshotRecorder{
		logger:              loglib.NewNoopLogger(),
		wrapped:             generator,
		store:               store,
		repeatableSnapshots: cfg.RepeatableSnapshots,
		schemaWorkers:       cfg.snapshotWorkers(),
	}

	for _, opt := range opts {
		opt(sr)
	}

	return sr
}

func (s *SnapshotRecorder) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	if err := s.filterOutExistingSnapshots(ctx, ss); err != nil {
		return err
	}

	// no tables to snapshot
	if !ss.HasTables() && !ss.HasSchemaOnlyTables() {
		return nil
	}

	requests := s.createRequests(ss)

	if err := s.markSnapshotInProgress(ctx, requests); err != nil {
		return err
	}

	err := s.wrapped.CreateSnapshot(ctx, ss)

	return s.markSnapshotCompleted(ctx, requests, err)
}

func (s *SnapshotRecorder) Close() error {
	s.store.Close()
	return s.wrapped.Close()
}

// createRequests records the snapshot scope as one request per schema and
// mode: data requests capture what the data snapshot covers, schema-only
// requests capture tables whose schema was replicated without their data.
// Recording the two modes separately is what allows a table moved between
// the two lists to be picked up correctly on the next run.
func (s *SnapshotRecorder) createRequests(ss *snapshot.Snapshot) []*snapshot.Request {
	requests := make([]*snapshot.Request, 0, len(ss.SchemaTables)+len(ss.SchemaOnlyTables))

	schemaOnlyWildcard := func(schema string) bool {
		return slices.Contains(ss.SchemaOnlyTables[schema], wildcard) || len(ss.SchemaOnlyTables[wildcard]) > 0
	}

	for schema, tables := range ss.SchemaTables {
		recorded := make([]string, 0, len(tables))
		for _, table := range tables {
			// a data wildcard fully covered by a schema-only wildcard copies
			// no data (only explicitly listed tables do), so don't record it
			// as data coverage
			if table == wildcard && schemaOnlyWildcard(schema) {
				continue
			}
			recorded = append(recorded, table)
		}
		if len(recorded) == 0 {
			continue
		}
		requests = append(requests, &snapshot.Request{
			Schema: schema,
			Tables: recorded,
			Mode:   snapshot.RequestModeData,
		})
	}

	for schema, tables := range ss.SchemaOnlyTables {
		recorded := make([]string, 0, len(tables))
		for _, table := range tables {
			// tables explicitly listed in the data snapshot get full
			// treatment, so they are covered by the data request
			if table != wildcard && slices.Contains(ss.SchemaTables[schema], table) {
				continue
			}
			recorded = append(recorded, table)
		}
		if len(recorded) == 0 {
			continue
		}
		requests = append(requests, &snapshot.Request{
			Schema: schema,
			Tables: recorded,
			Mode:   snapshot.RequestModeSchemaOnly,
		})
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
			// captured before MarkCompleted mutates it: if the store write
			// below fails, this is the status that remains persisted, and it is
			// the one an operator has to repair
			persistedStatus := req.Status
			req.MarkCompleted(req.Schema, schemaErrs)
			if updateErr := s.updateWithRetry(ctx, req); updateErr != nil {
				// A request left in a non-terminal state is never retried:
				// HasFailedForTable only reports a failure for a completed
				// request, so its tables are treated as already covered and
				// silently skipped by every subsequent run. Log it regardless of
				// how the error is routed below -- when err != nil the error is
				// folded into schemaErrs, which is written by the very store
				// call that just failed, so it would otherwise vanish. Name the
				// state to repair: without it an operator has the symptom but
				// not the row to fix.
				s.logger.Error(updateErr, "recording snapshot request completion, request stuck in non-terminal state and will never be retried: mark it completed with an error in the snapshot store to bring its tables back into scope", loglib.Fields{
					"severity":         "DATALOSS",
					"schema":           req.Schema,
					"mode":             req.GetMode(),
					"persisted_status": persistedStatus,
				})
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

const (
	// updateRetries bounds the attempts to persist a request's terminal state.
	// Losing that write strands the request: its tables are then treated as
	// already covered and skipped by every later run, so a transient store
	// error is worth a few retries rather than one shot. Kept small, and with a
	// short interval, so the attempts fit inside updateTimeout.
	updateRetries  = 3
	updateInterval = 500 * time.Millisecond
)

// updateWithRetry persists the request's terminal state, retrying a bounded
// number of times. The update is idempotent (it matches on schema, table names
// and mode, and skips rows already completed), so a retry after a partial
// failure cannot double-apply.
func (s *SnapshotRecorder) updateWithRetry(ctx context.Context, req *snapshot.Request) error {
	bo := backoff.NewConstantBackoff(ctx, &backoff.ConstantConfig{
		MaxRetries: updateRetries,
		Interval:   updateInterval,
	})

	return bo.RetryNotify(
		func() error { return s.store.UpdateSnapshotRequest(ctx, req) },
		func(err error, _ time.Duration) {
			s.logger.Warn(err, "retrying snapshot request update", loglib.Fields{
				"schema": req.Schema,
				"mode":   req.GetMode(),
			})
		})
}

func (s *SnapshotRecorder) filterOutExistingSnapshots(ctx context.Context, ss *snapshot.Snapshot) error {
	// if we want to be able to repeat snapshots, we don't filter out existing ones
	if s.repeatableSnapshots || (len(ss.SchemaTables) == 0 && len(ss.SchemaOnlyTables) == 0) {
		return nil
	}

	schemas := make(map[string]struct{}, len(ss.SchemaTables)+len(ss.SchemaOnlyTables))
	for schema := range ss.SchemaTables {
		schemas[schema] = struct{}{}
	}
	for schema := range ss.SchemaOnlyTables {
		schemas[schema] = struct{}{}
	}

	filteredData := make(map[string][]string, len(ss.SchemaTables))
	filteredSchemaOnly := make(map[string][]string, len(ss.SchemaOnlyTables))
	for schema := range schemas {
		snapshotRequests, err := s.store.GetSnapshotRequestsBySchema(ctx, schema)
		if err != nil {
			return fmt.Errorf("retrieving existing snapshots for schema %s: %w", schema, err)
		}

		dataRequests := make([]*snapshot.Request, 0, len(snapshotRequests))
		schemaOnlyRequests := make([]*snapshot.Request, 0, len(snapshotRequests))
		for _, req := range snapshotRequests {
			if req.GetMode() == snapshot.RequestModeSchemaOnly {
				schemaOnlyRequests = append(schemaOnlyRequests, req)
			} else {
				dataRequests = append(dataRequests, req)
			}
		}

		if tables, found := ss.SchemaTables[schema]; found {
			filtered := filterDataTables(schema, tables, dataRequests, schemaOnlyRequests, ss.SchemaOnlyTables)
			if len(filtered) > 0 {
				filteredData[schema] = filtered
			}
		}
		if tables, found := ss.SchemaOnlyTables[schema]; found {
			// a completed data request also covers the table's schema, so
			// the schema-only scope filters against requests of both modes
			filtered := filterTablesByRequests(tables, snapshotRequests)
			if len(filtered) > 0 {
				filteredSchemaOnly[schema] = filtered
			}
		}
	}

	if len(ss.SchemaTables) > 0 {
		ss.SchemaTables = filteredData
	}
	if len(ss.SchemaOnlyTables) > 0 {
		ss.SchemaOnlyTables = filteredSchemaOnly
	}
	return nil
}

// filterDataTables removes tables already covered by an existing data
// snapshot request. A completed wildcard data request does not cover tables
// that were recorded as schema-only at the time (their data was deliberately
// skipped): those are added back to the data scope once they are no longer
// configured as schema-only.
func filterDataTables(schema string, tables []string, dataRequests, schemaOnlyRequests []*snapshot.Request, currentSchemaOnly map[string][]string) []string {
	filtered := filterTablesByRequests(tables, dataRequests)

	// only when a wildcard entry was covered by a completed data request can
	// schema-only recorded tables be hiding behind it
	if !slices.Contains(tables, wildcard) || slices.Contains(filtered, wildcard) {
		return filtered
	}

	dataRecorded := map[string]struct{}{}
	for _, req := range dataRequests {
		for _, table := range req.Tables {
			dataRecorded[table] = struct{}{}
		}
	}
	currentSchemaOnlyContains := func(table string) bool {
		contains := func(tables []string) bool {
			return slices.Contains(tables, table) || slices.Contains(tables, wildcard)
		}
		return contains(currentSchemaOnly[schema]) || contains(currentSchemaOnly[wildcard])
	}

	for _, req := range schemaOnlyRequests {
		for _, table := range req.Tables {
			if table == wildcard {
				continue
			}
			if _, found := dataRecorded[table]; found {
				continue
			}
			if currentSchemaOnlyContains(table) {
				continue
			}
			if !slices.Contains(filtered, table) {
				filtered = append(filtered, table)
			}
		}
	}
	return filtered
}

// filterTablesByRequests removes tables already covered by any of the
// existing snapshot requests.
func filterTablesByRequests(tables []string, snapshotRequests []*snapshot.Request) []string {
	if len(snapshotRequests) == 0 {
		return tables
	}

	existingRequests := map[string]*snapshot.Request{}
	for _, req := range snapshotRequests {
		for _, table := range req.Tables {
			existingRequests[table] = req
		}
	}

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
	return filteredTables
}

func (c *Config) snapshotWorkers() uint {
	if c.SnapshotWorkers <= 0 {
		return defaultSnapshotWorkers
	}
	return c.SnapshotWorkers
}
