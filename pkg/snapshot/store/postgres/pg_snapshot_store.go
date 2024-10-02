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

func NewStore(ctx context.Context, url string) (*Store, error) {
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

func (s *Store) CreateSnapshotRequest(ctx context.Context, req *snapshot.Snapshot) error {
	query := fmt.Sprintf(`INSERT INTO %s (schema_name, table_name, identity_column_names, created_at, updated_at, status)
	VALUES($1, $2, $3,'now()','now()','requested')`, snapshotsTable())
	_, err := s.conn.Exec(ctx, query, req.SchemaName, req.TableName, pq.StringArray(req.IdentityColumnNames))
	if err != nil {
		return fmt.Errorf("error creating snapshot request: %w", err)
	}
	return nil
}

func (s *Store) UpdateSnapshotRequest(ctx context.Context, req *snapshot.Snapshot) error {
	errStr := ""
	if req.Error != nil {
		errStr = req.Error.Error()
	}
	query := fmt.Sprintf(`UPDATE %s SET status = '%s', error = '%s', updated_at = 'now()'
	WHERE schema_name = '%s' and table_name = '%s' and status != 'completed'`, snapshotsTable(), req.Status, errStr, req.SchemaName, req.TableName)
	_, err := s.conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("error updating snapshot request: %w", err)
	}
	return nil
}

func (s *Store) GetSnapshotRequests(ctx context.Context, status snapshot.Status) ([]*snapshot.Snapshot, error) {
	query := fmt.Sprintf(`SELECT schema_name,table_name,identity_column_names,status FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), status, queryLimit)
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot requests: %w", err)
	}
	defer rows.Close()

	snapshots := []*snapshot.Snapshot{}
	for rows.Next() {
		snapshot := &snapshot.Snapshot{}
		if err := rows.Scan(&snapshot.SchemaName, &snapshot.TableName, &snapshot.IdentityColumnNames, &snapshot.Status); err != nil {
			return nil, fmt.Errorf("scanning snapshot row: %w", err)
		}

		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

func (s *Store) createTable(ctx context.Context) error {
	createQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	req_id SERIAL PRIMARY KEY,
	schema_name TEXT,
	table_name TEXT,
	identity_column_names TEXT[],
	created_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE,
	status TEXT CHECK (status IN ('requested', 'started', 'completed')),
	error TEXT )`, snapshotsTable())
	_, err := s.conn.Exec(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("error creating snapshots postgres table: %w", err)
	}

	uniqueIndexQuery := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS schema_table_status_unique_index
	ON %s(schema_name,table_name) WHERE status != 'completed'`, snapshotsTable())
	_, err = s.conn.Exec(ctx, uniqueIndexQuery)
	if err != nil {
		return fmt.Errorf("error creating unique index on snapshots postgres table: %w", err)
	}

	return err
}

func snapshotsTable() string {
	return fmt.Sprintf("%s.%s", store.SchemaName, store.TableName)
}
