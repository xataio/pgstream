// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/store"
)

type Store struct {
	conn postgres.Querier
	// lockConnBuilder opens the dedicated connection that holds the request
	// locks. Session level advisory locks belong to one connection, so they
	// can't be taken through the pool.
	lockConnBuilder func(context.Context) (postgres.Querier, error)
}

const (
	queryLimit = 1000

	uniqueIndexName = "schema_table_status_hash_unique_index"
)

func New(ctx context.Context, url string) (*Store, error) {
	conn, err := postgres.NewConnPool(ctx, url)
	if err != nil {
		return nil, err
	}

	s := &Store{
		conn: conn,
		lockConnBuilder: func(ctx context.Context) (postgres.Querier, error) {
			return postgres.NewConn(ctx, url)
		},
	}

	// create snapshots table if it doesn't exist
	if err := s.createTable(ctx); err != nil {
		return nil, fmt.Errorf("creating snapshots table: %w", err)
	}

	return s, nil
}

func (s *Store) Close() error {
	return s.conn.Close(context.Background())
}

func (s *Store) CreateSnapshotRequest(ctx context.Context, req *snapshot.Request) error {
	query := fmt.Sprintf(`INSERT INTO %s (schema_name, table_names, created_at, updated_at, status, mode)
	VALUES($1, $2, now(), now(),'requested', $3)`, snapshotsTable())
	_, err := s.conn.Exec(ctx, query, req.Schema, pq.StringArray(req.Tables), req.GetMode())
	if err != nil {
		if isNonCompletedRequestConflict(err) {
			return fmt.Errorf("error creating snapshot request: %w: %w", store.ErrNonCompletedRequestExists, err)
		}
		return fmt.Errorf("error creating snapshot request: %w", err)
	}

	return nil
}

// isNonCompletedRequestConflict reports whether err is the unique violation
// on the index that allows one non-completed request per schema and table
// list. Other integrity errors (check or not null constraints) are not.
func isNonCompletedRequestConflict(err error) bool {
	constraintErr := &postgres.ErrConstraintViolation{}
	return errors.As(err, &constraintErr) && strings.Contains(constraintErr.Details, uniqueIndexName)
}

// LockSnapshotRequests takes a session level advisory lock per request, keyed
// like the unique index on the schema name and the table names hash (mode is
// not part of the key). The locks live on a dedicated connection, so they are
// released when release closes it, or by the server when the process dies and
// its connection drops.
func (s *Store) LockSnapshotRequests(ctx context.Context, reqs []*snapshot.Request) (func(), error) {
	conn, err := s.lockConnBuilder(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening snapshot request lock connection: %w", err)
	}
	release := func() {
		conn.Close(context.Background())
	}

	// take the locks in a fixed order so two runs over the same scopes
	// contend on the same first lock
	sorted := slices.Clone(reqs)
	slices.SortFunc(sorted, func(a, b *snapshot.Request) int {
		return strings.Compare(lockKey(a), lockKey(b))
	})

	query := fmt.Sprintf(`SELECT pg_try_advisory_lock(hashtextextended($1::text || '/' || %s.snapshot_table_names_hash($2::text[]), 0))`, store.SchemaName)
	for _, req := range sorted {
		var locked bool
		if err := conn.QueryRow(ctx, []any{&locked}, query, req.Schema, pq.StringArray(req.Tables)); err != nil {
			release()
			return nil, fmt.Errorf("locking snapshot request for schema %q: %w", req.Schema, err)
		}
		if !locked {
			release()
			return nil, fmt.Errorf("schema %q tables %v: %w", req.Schema, req.Tables, store.ErrSnapshotRequestLocked)
		}
	}

	return release, nil
}

func lockKey(req *snapshot.Request) string {
	return req.Schema + "/" + strings.Join(req.Tables, ",")
}

// ResumeSnapshotRequest takes over the non-completed request matching the
// unique index key (schema name and table names, any mode), marks it in
// progress and records the mode on input, so later updates keyed on mode
// match it.
func (s *Store) ResumeSnapshotRequest(ctx context.Context, req *snapshot.Request) (bool, error) {
	query := fmt.Sprintf(`UPDATE %s SET status = 'in progress', mode = $1, errors = NULL, updated_at = now()
	WHERE schema_name = $2 and table_names = $3 and status != 'completed'`, snapshotsTable())
	tag, err := s.conn.Exec(ctx, query, req.GetMode(), req.Schema, pq.StringArray(req.Tables))
	if err != nil {
		return false, fmt.Errorf("error resuming snapshot request: %w", err)
	}

	return tag.RowsAffected() > 0, nil
}

func (s *Store) UpdateSnapshotRequest(ctx context.Context, req *snapshot.Request) error {
	query := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, updated_at = now()
	WHERE schema_name = $3 and table_names = $4 and mode = $5 and status != 'completed'`, snapshotsTable())
	_, err := s.conn.Exec(ctx, query, req.Status, req.Errors, req.Schema, pq.StringArray(req.Tables), req.GetMode())
	if err != nil {
		return fmt.Errorf("error updating snapshot request: %w", err)
	}

	return nil
}

func (s *Store) GetSnapshotRequestsByStatus(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
	query := fmt.Sprintf(`SELECT schema_name,table_names,status,mode,errors FROM %s
	WHERE status = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
	rows, err := s.conn.Query(ctx, query, status)
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot requests by status: %w", err)
	}
	defer rows.Close()

	snapshotRequests := []*snapshot.Request{}
	for rows.Next() {
		req := &snapshot.Request{}
		if err := rows.Scan(&req.Schema, &req.Tables, &req.Status, &req.Mode, &req.Errors); err != nil {
			return nil, fmt.Errorf("scanning snapshot row: %w", err)
		}

		snapshotRequests = append(snapshotRequests, req)
	}

	return snapshotRequests, nil
}

func (s *Store) GetSnapshotRequestsBySchema(ctx context.Context, schema string) ([]*snapshot.Request, error) {
	query := fmt.Sprintf(`SELECT schema_name,table_names,status,mode,errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
	rows, err := s.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot requests: %w", err)
	}
	defer rows.Close()

	snapshotRequests := []*snapshot.Request{}
	for rows.Next() {
		req := &snapshot.Request{}
		if err := rows.Scan(&req.Schema, &req.Tables, &req.Status, &req.Mode, &req.Errors); err != nil {
			return nil, fmt.Errorf("scanning snapshot request row: %w", err)
		}

		snapshotRequests = append(snapshotRequests, req)
	}

	return snapshotRequests, nil
}

func (s *Store) createTable(ctx context.Context) error {
	createSchemaQuery := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, store.SchemaName)
	_, err := s.conn.Exec(ctx, createSchemaQuery)
	if err != nil {
		return fmt.Errorf("error creating pgstream schema for snapshot store: %w", err)
	}

	createTableQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	req_id SERIAL PRIMARY KEY,
	schema_name TEXT,
	table_names TEXT[],
	created_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE,
	status TEXT CHECK (status IN ('requested', 'in progress', 'completed')),
	mode TEXT NOT NULL DEFAULT 'data' CHECK (mode IN ('data', 'schema-only')),
	errors JSONB )`, snapshotsTable())
	_, err = s.conn.Exec(ctx, createTableQuery)
	if err != nil {
		return fmt.Errorf("error creating snapshots postgres table: %w", err)
	}

	// upgrade tables created before the mode column existed; all requests
	// recorded back then were data snapshot requests
	addModeColumnQuery := fmt.Sprintf(`ALTER TABLE %s
	ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'data' CHECK (mode IN ('data', 'schema-only'))`, snapshotsTable())
	_, err = s.conn.Exec(ctx, addModeColumnQuery)
	if err != nil {
		return fmt.Errorf("error adding mode column to snapshots postgres table: %w", err)
	}

	// the hashed index below keys on a fixed-size digest of the table names
	// instead of the raw table_names array, whose index row overflows the btree
	// tuple size limit for schemas with many tables. postgres requires an index
	// expression to be IMMUTABLE, so wrap the digest in an immutable helper (a
	// text[] digest is deterministic).
	hashFuncQuery := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s.snapshot_table_names_hash(names TEXT[])
	RETURNS TEXT LANGUAGE sql IMMUTABLE AS $func$ SELECT md5(names::text) $func$`, store.SchemaName)
	_, err = s.conn.Exec(ctx, hashFuncQuery)
	if err != nil {
		return fmt.Errorf("error creating table names hash function for snapshots postgres table: %w", err)
	}

	// create the hashed index under a new name, then drop the legacy raw-array
	// index. both statements are idempotent (IF NOT EXISTS / IF EXISTS), so once
	// migrated a startup neither creates nor drops anything, and during the
	// one-time migration the new index is in place before the old one is removed,
	// so a unique constraint is always enforced (no window without it).
	uniqueIndexQuery := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS schema_table_status_hash_unique_index
	ON %s(schema_name,%s.snapshot_table_names_hash(table_names)) WHERE status != 'completed'`, snapshotsTable(), store.SchemaName)
	_, err = s.conn.Exec(ctx, uniqueIndexQuery)
	if err != nil {
		return fmt.Errorf("error creating unique index on snapshots postgres table: %w", err)
	}

	// the legacy index indexed the raw table_names array
	dropLegacyIndexQuery := fmt.Sprintf(`DROP INDEX IF EXISTS %s.schema_table_status_unique_index`, store.SchemaName)
	_, err = s.conn.Exec(ctx, dropLegacyIndexQuery)
	if err != nil {
		return fmt.Errorf("error dropping legacy unique index on snapshots postgres table: %w", err)
	}

	return err
}

func snapshotsTable() string {
	return postgres.QuoteQualifiedIdentifier(store.SchemaName, store.TableName)
}
