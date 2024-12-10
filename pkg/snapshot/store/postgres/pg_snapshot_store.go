// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/store"
)

type Store struct {
	conn postgres.Querier
}

const queryLimit = 1000

func New(ctx context.Context, url string) (*Store, error) {
	conn, err := postgres.NewConnPool(ctx, url)
	if err != nil {
		return nil, err
	}

	s := &Store{
		conn: conn,
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
	query := fmt.Sprintf(`INSERT INTO %s (schema_name, table_names, created_at, updated_at, status)
	VALUES($1, $2, now(), now(),'requested')`, snapshotsTable())
	_, err := s.conn.Exec(ctx, query, req.Snapshot.SchemaName, pq.StringArray(req.Snapshot.TableNames))
	if err != nil {
		return fmt.Errorf("error creating snapshot request: %w", err)
	}
	return nil
}

func (s *Store) UpdateSnapshotRequest(ctx context.Context, req *snapshot.Request) error {
	query := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, updated_at = now()
	WHERE schema_name = $3 and table_names = $4 and status != 'completed'`, snapshotsTable())
	_, err := s.conn.Exec(ctx, query, req.Status, req.Errors, req.Snapshot.SchemaName, pq.StringArray(req.Snapshot.TableNames))
	if err != nil {
		return fmt.Errorf("error updating snapshot request: %w", err)
	}
	return nil
}

func (s *Store) GetSnapshotRequestsByStatus(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
	query := fmt.Sprintf(`SELECT schema_name,table_names,status,errors FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), status, queryLimit)
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot requests by status: %w", err)
	}
	defer rows.Close()

	snapshotRequests := []*snapshot.Request{}
	for rows.Next() {
		req := &snapshot.Request{}
		if err := rows.Scan(&req.Snapshot.SchemaName, &req.Snapshot.TableNames, &req.Status, &req.Errors); err != nil {
			return nil, fmt.Errorf("scanning snapshot row: %w", err)
		}

		snapshotRequests = append(snapshotRequests, req)
	}

	return snapshotRequests, nil
}

func (s *Store) GetSnapshotRequestsBySchema(ctx context.Context, schema string) ([]*snapshot.Request, error) {
	query := fmt.Sprintf(`SELECT schema_name,table_names,status,errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
	rows, err := s.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot requests: %w", err)
	}
	defer rows.Close()

	snapshotRequests := []*snapshot.Request{}
	for rows.Next() {
		req := &snapshot.Request{}
		if err := rows.Scan(&req.Snapshot.SchemaName, &req.Snapshot.TableNames, &req.Status, &req.Errors); err != nil {
			return nil, fmt.Errorf("scanning snapshot request row: %w", err)
		}

		snapshotRequests = append(snapshotRequests, req)
	}

	return snapshotRequests, nil
}

func (s *Store) createTable(ctx context.Context) error {
	createQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	req_id SERIAL PRIMARY KEY,
	schema_name TEXT,
	table_names TEXT[],
	created_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE,
	status TEXT CHECK (status IN ('requested', 'in progress', 'completed')),
	errors JSONB )`, snapshotsTable())
	_, err := s.conn.Exec(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("error creating snapshots postgres table: %w", err)
	}

	uniqueIndexQuery := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS schema_table_status_unique_index
	ON %s(schema_name,table_names) WHERE status != 'completed'`, snapshotsTable())
	_, err = s.conn.Exec(ctx, uniqueIndexQuery)
	if err != nil {
		return fmt.Errorf("error creating unique index on snapshots postgres table: %w", err)
	}

	return err
}

func snapshotsTable() string {
	return fmt.Sprintf("%s.%s", pq.QuoteIdentifier(store.SchemaName), pq.QuoteIdentifier(store.TableName))
}
